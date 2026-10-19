//! Sink writes: column mapping, overwrite mode, and a plan shape that must be rejected.
use crate::common;

use std::sync::Arc;

use common::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};

/// An existing sink whose columns match by name but in another order gets every value in the
/// right column.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn existing_sink_with_other_column_order_is_written_by_name() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 1, 20).await;
    // The sink exists with the same columns in another order (e.g. created by an earlier
    // version of the job that selected `b, a`).
    let sink = Arc::new(Schema::new(vec![Field::new("b", DataType::Int64, true), Field::new("a", DataType::Int64, true)]));
    create_table(&out, &sink, &[]).await;
    let sql = format!("INSERT INTO delta('{}') SELECT id AS a, cid AS b FROM delta('{}')", loc(&out), loc(&src));
    run_once(&sql, 1).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
}

/// Two overlapping `--once` runs of an overwrite job (cron overlap, retried pod): the loser
/// recomputes its removes on the winner's snapshot, so exactly one result survives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overlapping_overwrite_runs_leave_one_result() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 10).await;
    let sql = format!(
        "SET nodra.job_name = 'ow2'; INSERT INTO delta('{}', mode => 'overwrite') SELECT id FROM delta('{}')",
        loc(&out),
        loc(&src)
    );
    run_once(&sql, 1).await.unwrap();
    for round in 0..5 {
        let (a, b) = tokio::join!(run_once(&sql, 1), run_once(&sql, 1));
        a.unwrap();
        b.unwrap();
        let n = table_rows(&loc(&out), &["id".to_string()]).await.len();
        assert_eq!(n, 20, "round {round}: overwrite sink has {n} rows");
    }
}

/// A CTE that references the driving source twice would make each worker self-join only its
/// own files: the job is rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn self_join_of_driving_source_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 3, 30).await;
    let sql = format!(
        "INSERT INTO delta('{}') WITH e AS (SELECT id, cid FROM delta('{}')) \
         SELECT a.id AS a_id, b.id AS b_id FROM e a JOIN e b ON a.id + 1 = b.id",
        loc(&out),
        loc(&src)
    );
    let err = run_once(&sql, 2).await.unwrap_err();
    assert!(matches!(err, nodra::NodraError::Rejected(_)), "{err}");
}
