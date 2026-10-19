//! Kafka stream source (feature `kafka`), built on librdkafka via `rdkafka`.
//!
//! Partitions are assigned manually (`assign`, not consumer-group subscription): Nodra decides
//! which worker reads which partition and keeps offsets in the sink's Delta log, so Kafka's
//! group coordination and offset commits are not used.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::error::KafkaError;
use rdkafka::message::Message;
use rdkafka::{Offset, TopicPartitionList};

use super::stream::{Fetched, StreamSource};
use crate::error::{NodraError, Result};

const TIMEOUT: Duration = Duration::from_secs(30);

/// A Kafka topic read with manual partition assignment.
pub struct KafkaSource {
    topic: String,
    consumer: Arc<Mutex<BaseConsumer>>,
}

impl std::fmt::Debug for KafkaSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaSource").field("topic", &self.topic).finish()
    }
}

impl KafkaSource {
    /// Connect to `brokers`; `properties` are passed to librdkafka unchanged.
    pub fn new(topic: &str, brokers: &str, group: &str, properties: &BTreeMap<String, String>) -> Result<Self> {
        let mut cfg = ClientConfig::new();
        cfg.set("bootstrap.servers", brokers)
            .set("group.id", group)
            .set("enable.auto.commit", "false")
            .set("enable.auto.offset.store", "false")
            .set("enable.partition.eof", "true")
            .set("auto.offset.reset", "error");
        for (k, v) in properties {
            cfg.set(k, v);
        }
        let consumer: BaseConsumer = cfg.create().map_err(kerr)?;
        Ok(Self { topic: topic.to_string(), consumer: Arc::new(Mutex::new(consumer)) })
    }

    async fn blocking<T, F>(&self, f: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&BaseConsumer, &str) -> Result<T> + Send + 'static,
    {
        let c = self.consumer.clone();
        let topic = self.topic.clone();
        tokio::task::spawn_blocking(move || {
            let guard = c.lock().map_err(|_| NodraError::Source("kafka consumer lock poisoned".into()))?;
            f(&guard, &topic)
        })
        .await
        .map_err(|e| NodraError::Source(format!("kafka task failed: {e}")))?
    }
}

fn kerr(e: KafkaError) -> NodraError {
    NodraError::Source(format!("kafka: {e}"))
}

#[async_trait]
impl StreamSource for KafkaSource {
    async fn partitions(&self) -> Result<Vec<i32>> {
        self.blocking(|c, topic| {
            let md = c.fetch_metadata(Some(topic), TIMEOUT).map_err(kerr)?;
            let t = md
                .topics()
                .iter()
                .find(|t| t.name() == topic)
                .ok_or_else(|| NodraError::Source(format!("kafka: topic `{topic}` not found")))?;
            if let Some(err) = t.error() {
                return Err(NodraError::Source(format!("kafka: topic `{topic}`: {err:?}")));
            }
            let mut ids: Vec<i32> = t.partitions().iter().map(|p| p.id()).collect();
            ids.sort_unstable();
            Ok(ids)
        })
        .await
    }

    async fn watermarks(&self, partition: i32) -> Result<(i64, i64)> {
        self.blocking(move |c, topic| c.fetch_watermarks(topic, partition, TIMEOUT).map_err(kerr))
            .await
    }

    async fn fetch(&self, partition: i32, from: i64, until: i64, max: usize) -> Result<Fetched> {
        self.blocking(move |c, topic| {
            let mut out = Fetched { payloads: Vec::new(), first_offset: from, next_offset: from };
            if from >= until || max == 0 {
                return Ok(out);
            }
            let mut tpl = TopicPartitionList::new();
            tpl.add_partition_offset(topic, partition, Offset::Offset(from)).map_err(kerr)?;
            c.assign(&tpl).map_err(kerr)?;
            let deadline = Instant::now() + TIMEOUT;
            let mut expected = from;
            while out.payloads.len() < max && expected < until {
                if Instant::now() > deadline {
                    return Err(NodraError::Source(format!(
                        "kafka: timed out reading {topic}[{partition}] at offset {expected}"
                    )));
                }
                match c.poll(Duration::from_millis(500)) {
                    None => continue,
                    Some(Err(KafkaError::PartitionEOF(_))) => break,
                    Some(Err(e)) => return Err(kerr(e)),
                    Some(Ok(m)) => {
                        if m.partition() != partition {
                            continue;
                        }
                        let off = m.offset();
                        if off < expected {
                            continue;
                        }
                        if off >= until {
                            break;
                        }
                        // Offsets can have gaps (compaction, transaction markers): we only
                        // require monotonic progress, never re-reading or skipping a message.
                        out.payloads.push(m.payload().map(<[u8]>::to_vec));
                        expected = off + 1;
                    }
                }
            }
            out.next_offset = expected;
            c.unassign().map_err(kerr)?;
            Ok(out)
        })
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(kv: &[(&str, &str)]) -> BTreeMap<String, String> {
        kv.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    #[test]
    fn creates_consumer_without_broker_contact() {
        // librdkafka connects lazily, so construction validates the configuration only.
        KafkaSource::new("t", "127.0.0.1:1", "g", &BTreeMap::new()).unwrap();
        let err = KafkaSource::new("t", "127.0.0.1:1", "g", &props(&[("no.such.property", "1")])).unwrap_err();
        assert!(err.to_string().contains("no.such.property"), "{err}");
    }

    #[cfg(feature = "kafka-ssl")]
    #[test]
    fn supports_sasl_ssl() {
        let p = props(&[
            ("security.protocol", "SASL_SSL"),
            ("sasl.mechanism", "PLAIN"),
            ("sasl.username", "$ConnectionString"),
            ("sasl.password", "Endpoint=sb://example.servicebus.windows.net/"),
        ]);
        KafkaSource::new("t", "example.servicebus.windows.net:9093", "g", &p).unwrap();
    }
}
