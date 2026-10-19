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
