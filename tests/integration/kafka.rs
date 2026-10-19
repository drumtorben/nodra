//! The Kafka source against librdkafka's in-process mock cluster (real Kafka protocol over TCP).
//! Run with `cargo test --features kafka --test integration kafka::`.

use crate::common;

use std::time::Duration;

use common::*;
use nodra::planner::JobClass;
use rdkafka::mocking::MockCluster;
use rdkafka::producer::{BaseProducer, BaseRecord, Producer};
use rdkafka::ClientConfig;

fn produce(bootstrap: &str, topic: &str, partition: i32, lines: &[String]) {
    let producer: BaseProducer = ClientConfig::new().set("bootstrap.servers", bootstrap).create().unwrap();
    for line in lines {
        producer.send(BaseRecord::<(), str>::to(topic).partition(partition).payload(line.as_str())).unwrap();
    }
    producer.flush(Duration::from_secs(30)).unwrap();
}

fn job(bootstrap: &str, topic: &str, dim: &std::path::Path, out: &std::path::Path, extra: &str) -> String {
    format!(
        "SET nodra.max_records_per_partition = '9'; {extra} \
         INSERT INTO delta('{}') \
         SELECT d.brand, count(*) AS clicks, count(DISTINCT k.user) AS users \
         FROM kafka('{topic}', brokers => '{bootstrap}', schema => 'id BIGINT, cid BIGINT, user VARCHAR') k \
         JOIN delta('{}') d ON k.cid = d.cid GROUP BY d.brand",
        loc(out),
        loc(dim)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_aggregation_is_exactly_once_across_runs_and_rescales() {
    let cluster = MockCluster::new(1).unwrap();
    let bootstrap = cluster.bootstrap_servers();
    cluster.create_topic("clicks", 4, 1).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (dimp, out) = (dir.path().join("dim"), dir.path().join("out"));
    dim_table(&dimp).await;

    let mut all = Vec::new();
    for p in 0..4 {
        let (lines, batch) = clicks(p as i64 * 100..p as i64 * 100 + 20);
        produce(&bootstrap, "clicks", p, &lines);
        all.push(batch);
    }
    let sql = job(&bootstrap, "clicks", &dimp, &out, "");
    let p = run_once(&sql, 2).await.unwrap();
    assert_eq!(p.plan.class, JobClass::MergeableAggregation);
    assert_matches_reference(&sql, &[concat(&all)]).await;

    // New messages in two partitions; a different worker count (stream progress is per partition).
    for p in [1, 3] {
        let (lines, batch) = clicks(p as i64 * 100 + 20..p as i64 * 100 + 33);
        produce(&bootstrap, "clicks", p, &lines);
        all.push(batch);
    }
    run_once(&sql, 3).await.unwrap();
    assert_matches_reference(&sql, &[concat(&all)]).await;
    // Nothing new: another run changes nothing.
    run_once(&sql, 1).await.unwrap();
    assert_matches_reference(&sql, &[concat(&all)]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_start_latest_and_bad_messages() {
    let cluster = MockCluster::new(1).unwrap();
    let bootstrap = cluster.bootstrap_servers();
    cluster.create_topic("events", 2, 1).unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (dimp, out) = (dir.path().join("dim"), dir.path().join("out"));
    dim_table(&dimp).await;
    // Existing messages are skipped with start = latest.
    produce(&bootstrap, "events", 0, &clicks(0..10).0);
    let sql = job(
        &bootstrap,
        "events",
        &dimp,
        &out,
        "SET nodra.start = 'latest'; SET nodra.bad_messages = 'skip';",
    );
    run_once(&sql, 1).await.unwrap();
    let (mut lines, batch) = clicks(10..25);
    lines.insert(3, "not json".to_string());
    produce(&bootstrap, "events", 1, &lines);
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[batch]).await;
}

/// A transactional producer writes a commit marker after its messages; it occupies an offset but
/// is never delivered. Reading up to the high watermark must still end at the high watermark
/// (otherwise `nodra_lag` never reaches 0). librdkafka's mock cluster writes no markers, so this
/// test needs a real broker: `NODRA_TEST_KAFKA=localhost:9092` (skipped without it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kafka_fetch_skips_transaction_markers() {
    use nodra::source::kafka::KafkaSource;
    use nodra::source::stream::StreamSource;

    let Ok(bootstrap) = std::env::var("NODRA_TEST_KAFKA") else {
        eprintln!("NODRA_TEST_KAFKA not set; skipped");
        return;
    };
    let topic = format!("nodra-tx-{}", std::process::id());
    let producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", &bootstrap)
        .set("transactional.id", &topic)
        .create()
        .unwrap();
    producer.init_transactions(Duration::from_secs(30)).unwrap();
    producer.begin_transaction().unwrap();
    for line in &clicks(0..5).0 {
        producer.send(BaseRecord::<(), str>::to(&topic).partition(0).payload(line.as_str())).unwrap();
    }
    producer.commit_transaction(Duration::from_secs(30)).unwrap();

    let src = KafkaSource::new(&topic, &bootstrap, "nodra-test", &Default::default()).unwrap();
    let (low, high) = src.watermarks(0).await.unwrap();
    assert_eq!(high - low, 6, "5 messages and one commit marker");
    let fetched = src.fetch(0, low, high, 100).await.unwrap();
    assert_eq!(fetched.payloads.len(), 5);
    assert_eq!(fetched.next_offset, high);
    // Nothing left to read: the next fetch returns nothing and stays at the end.
    let again = src.fetch(0, fetched.next_offset, high, 100).await.unwrap();
    assert!(again.payloads.is_empty());
    assert_eq!(again.next_offset, high);

    // An open transaction is not readable yet: its messages must not be skipped.
    producer.begin_transaction().unwrap();
    for line in &clicks(5..8).0 {
        producer.send(BaseRecord::<(), str>::to(&topic).partition(0).payload(line.as_str())).unwrap();
    }
    producer.flush(Duration::from_secs(30)).unwrap();
    let (_, open_high) = src.watermarks(0).await.unwrap();
    let pending = src.fetch(0, high, open_high, 100).await.unwrap();
    assert!(pending.payloads.is_empty());
    assert_eq!(pending.next_offset, high);
    producer.commit_transaction(Duration::from_secs(30)).unwrap();
    // The commit marker is written asynchronously (until then the high watermark, which is the
    // last stable offset under read_committed, does not move): read until the messages become
    // visible, and never move past them while they are not.
    let (mut from, mut read, mut end) = (pending.next_offset, 0, 0);
    for _ in 0..100 {
        end = src.watermarks(0).await.unwrap().1;
        let f = src.fetch(0, from, end, 100).await.unwrap();
        read += f.payloads.len();
        from = f.next_offset;
        if read == 3 {
            break;
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    assert_eq!((read, from), (3, end), "3 messages and one commit marker");
}
