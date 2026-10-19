//! Fencing of old workers after a rescale, and class-4 jobs under two worker counts.
use crate::common;

use std::sync::Arc;

use common::*;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use deltalake_core::kernel::Transaction;
use nodra::delta::{self, CommitOutcome};
use nodra::exec::Pipeline;
use nodra::job::Job;

/// The fence: an old worker whose read snapshot predates the new generation's first commit
/// cannot commit its progress key any more (delta-rs `ConcurrentTransaction`), however many
/// commits landed in between.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_worker_commit_from_before_the_fence_conflicts() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 20).await;
    let sql = format!(
        "SET nodra.job_name = 'fenceprobe'; INSERT INTO delta('{}') SELECT id FROM delta('{}')",
        loc(&out),
        loc(&src)
    );
    run_once(&sql, 2).await.unwrap();
    let read = delta::version(&delta::open(&loc(&out)).await.unwrap()).unwrap();
    for k in 0..3 {
        append(&src, events(100 + k * 10..110 + k * 10)).await;
        run_once(&sql, 3).await.unwrap(); // generation 1: first commit fences, then more commits
    }
    let p = Pipeline::prepare(Job::parse(&sql, "x").unwrap()).await.unwrap();
    for j in 0..2 {
        let mut t = delta::open_version(&loc(&out), read as i64).await.unwrap();
        let key = p.delta_progress_key(0, j, 2);
        let outcome =
            delta::commit(&mut t, vec![], vec![Transaction::new(&key, 99)], delta::append_op(&[]), 50)
                .await
                .unwrap();
        assert!(matches!(outcome, CommitOutcome::Conflict), "old worker {j}: {outcome:?}");
    }
}

fn spend(days: &[&str], per_day: i64) -> RecordBatch {
    let mut day = Vec::new();
    let mut cid = Vec::new();
    let mut w = Vec::new();
    for d in days {
        for c in 0..per_day {
            day.push(d.to_string());
            cid.push(c);
            w.push(c as f64 * 1.5);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("cid", DataType::Int64, true),
            Field::new("w", DataType::Float64, true),
            Field::new("day", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(cid)) as ArrayRef,
            Arc::new(Float64Array::from(w)),
            Arc::new(StringArray::from(day)),
        ],
    )
    .unwrap()
}

/// Class 4 progress is per partition, so two worker counts running at the same time (a rolling
/// update) may both pick a partition, but only one commit of it succeeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class4_two_worker_counts_at_the_same_time() {
    for round in 0..3 {
        let dir = tempfile::tempdir().unwrap();
        let (a, b, out) = (dir.path().join("a"), dir.path().join("b"), dir.path().join("out"));
        events_table(&a, 3, 30).await;
        let sb = spend(&["2026-10-10"], 3).schema();
        create_table(&b, &sb, &["day"]).await;
        append(&b, spend(&["2026-10-10", "2026-10-11", "2026-10-12"], 7)).await;
        let sql = format!(
            "SET nodra.job_name = 'c4roll{round}'; SET nodra.broadcast_threshold = '1KB'; SET nodra.max_files_per_batch = '1'; \
             INSERT INTO delta('{}', partition_by => 'day') \
             SELECT a.day, a.cid, sum(a.v) AS v, max(b.w) AS w FROM delta('{}') a JOIN delta('{}') b \
             ON a.day = b.day AND a.cid = b.cid GROUP BY a.day, a.cid",
            loc(&out),
            loc(&a),
            loc(&b)
        );
        let (s2, s3) = (sql.clone(), sql.clone());
        let x = tokio::spawn(async move { run_once(&s2, 2).await.map(|_| ()) });
        let y = tokio::spawn(async move { run_once(&s3, 3).await.map(|_| ()) });
        x.await.unwrap().unwrap();
        y.await.unwrap().unwrap();
        assert_matches_reference(&sql, &[]).await;
    }
}
