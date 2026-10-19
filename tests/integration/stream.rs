use crate::common;

use common::*;
use nodra::planner::JobClass;

fn job(stream: &std::path::Path, dim: &std::path::Path, out: &std::path::Path) -> String {
    format!(
        "SET nodra.max_records_per_partition = '7'; \
         INSERT INTO delta('{}') \
         SELECT d.brand, count(*) AS clicks, count(DISTINCT s.user) AS users \
         FROM jsonl_stream('{}', schema => 'id BIGINT, cid BIGINT, user VARCHAR') s \
         JOIN delta('{}') d ON s.cid = d.cid GROUP BY d.brand",
        loc(out),
        loc(stream),
        loc(dim)
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_aggregation_is_exactly_once_across_runs() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, dimp, out) = (dir.path().join("clicks"), dir.path().join("dim"), dir.path().join("out"));
    dim_table(&dimp).await;
    let mut all = Vec::new();
    // 5 partitions, 2 workers, small fetch size => several batches per run.
    for p in 0..5 {
        let (lines, batch) = clicks(p * 100..p * 100 + 23);
        append_jsonl(&stream, p as i32, &lines);
        all.push(batch);
    }
    let sql = job(&stream, &dimp, &out);
    let p = run_once(&sql, 2).await.unwrap();
    assert_eq!(p.plan.class, JobClass::MergeableAggregation);
    assert_matches_reference(&sql, &[concat(&all)]).await;

    // More data in some partitions only.
    for p in [1, 3] {
        let (lines, batch) = clicks(p * 100 + 23..p * 100 + 40);
        append_jsonl(&stream, p as i32, &lines);
        all.push(batch);
    }
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[concat(&all)]).await;
    // A run without new data changes nothing.
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[concat(&all)]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_rescaling_is_allowed() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    let mut all = Vec::new();
    for p in 0..4 {
        let (lines, batch) = clicks(p * 100..p * 100 + 10);
        append_jsonl(&stream, p as i32, &lines);
        all.push(batch);
    }
    let sql = format!(
        "INSERT INTO delta('{}') SELECT id, cid FROM jsonl_stream('{}', schema => 'id BIGINT, cid BIGINT, user VARCHAR')",
        loc(&out),
        loc(&stream)
    );
    run_once(&sql, 2).await.unwrap();
    for p in 0..4 {
        let (lines, batch) = clicks(p * 100 + 10..p * 100 + 15);
        append_jsonl(&stream, p as i32, &lines);
        all.push(batch);
    }
    // Progress is per partition, so a different worker count continues seamlessly.
    run_once(&sql, 3).await.unwrap();
    assert_eq!(assert_matches_reference(&sql, &[concat(&all)]).await, 60);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_start_latest_skips_existing_data() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    let (old, _) = clicks(0..10);
    append_jsonl(&stream, 0, &old);
    let sql = format!(
        "SET nodra.start = 'latest'; INSERT INTO delta('{}') SELECT id FROM jsonl_stream('{}', schema => 'id BIGINT')",
        loc(&out),
        loc(&stream)
    );
    run_once(&sql, 1).await.unwrap();
    let (new, batch) = clicks(10..14);
    append_jsonl(&stream, 0, &new);
    run_once(&sql, 1).await.unwrap();
    let ids = table_rows(&loc(&out), &["id".to_string()]).await;
    let expected = render(&[batch.project(&[0]).unwrap()]);
    assert_eq!(ids, expected);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn malformed_message_fails_with_context() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    append_jsonl(&stream, 0, &[r#"{"id": 1}"#.to_string(), "not json".to_string()]);
    let sql = format!(
        "INSERT INTO delta('{}') SELECT id FROM jsonl_stream('{}', schema => 'id BIGINT')",
        loc(&out),
        loc(&stream)
    );
    let err = run_once(&sql, 1).await.unwrap_err().to_string();
    assert!(err.contains("partition 0") && err.contains("message #1"), "{err}");
    // A value that is valid JSON but not a valid value for the column type.
    append_jsonl(&stream, 1, &[r#"{"id": 2}"#.to_string(), r#"{"id": "two"}"#.to_string()]);
    let skip = format!("SET nodra.bad_messages = 'skip'; {sql}");
    run_once(&skip, 1).await.unwrap();
    let ids = table_rows(&loc(&out), &["id".to_string()]).await;
    assert_eq!(ids, vec!["1".to_string(), "2".to_string()]);
}
