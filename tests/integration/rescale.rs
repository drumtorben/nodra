//! Changing the worker count of Delta-driven jobs (worker generations, see `nodra::rescale`).
use crate::common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use nodra::exec::{self, Pipeline, WorkerId};
use nodra::job::Job;
use tokio::sync::watch;

/// Run only the workers `indices` of `count`.
async fn run_some(sql: &str, indices: &[u32], count: u32) -> nodra::Result<()> {
    let p = Arc::new(Pipeline::prepare(Job::parse(sql, "test")?).await?);
    let (_tx, rx) = watch::channel(false);
    exec::run(p, indices.to_vec(), count, true, rx).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scale_up_and_down_without_duplicates() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out, agg) = (dir.path().join("events"), dir.path().join("out"), dir.path().join("agg"));
    events_table(&src, 3, 20).await;
    let rows = format!("SET nodra.job_name = 'rows'; INSERT INTO delta('{}') SELECT id, cid, v FROM delta('{}')", loc(&out), loc(&src));
    let sums = format!(
        "SET nodra.job_name = 'sums'; INSERT INTO delta('{}', partition_by => 'day') \
         SELECT day, cid, count(*) AS n, sum(v) AS s FROM delta('{}') GROUP BY day, cid",
        loc(&agg),
        loc(&src)
    );
    let mut next = 1000;
    for workers in [2, 3, 5, 3, 1, 4] {
        for sql in [&rows, &sums] {
            run_once(sql, workers).await.unwrap();
            assert_matches_reference(sql, &[]).await;
        }
        append(&src, events(next..next + 25)).await;
        next += 25;
    }
    let status = nodra::status::render(&Pipeline::prepare(Job::parse(&rows, "x").unwrap()).await.unwrap())
        .await
        .unwrap();
    assert!(status.contains("workers: 1 (generation 4, previous run)"), "{status}");
    assert!(status.contains("workers: 4 (generation 5, current)"), "{status}");
}

/// The previous workers stopped at different source versions: the new generation starts at the
/// smallest one and skips what each old owner already processed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_that_stopped_at_different_versions() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 20).await;
    let sql = format!("INSERT INTO delta('{}') SELECT id, cid FROM delta('{}')", loc(&out), loc(&src));
    run_once(&sql, 2).await.unwrap();
    for k in 0..4 {
        append(&src, events(100 + k * 10..110 + k * 10)).await;
        // Only worker 0 keeps running: it gets ahead of worker 1.
        run_some(&sql, &[0], 2).await.unwrap();
    }
    append(&src, events(500..530)).await;
    run_once(&sql, 3).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescaling_waits_for_every_old_worker() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 20).await;
    let sql = format!("INSERT INTO delta('{}') SELECT id FROM delta('{}')", loc(&out), loc(&src));
    // Worker 1 of 2 never ran: its files are unprocessed, so 3 workers cannot take over yet.
    run_some(&sql, &[0], 2).await.unwrap();
    let err = run_once(&sql, 3).await.unwrap_err().to_string();
    assert!(err.contains("never committed"), "{err}");
    run_some(&sql, &[1], 2).await.unwrap();
    run_once(&sql, 3).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
}

/// A worker of the old count that keeps running after the job was restarted with a new count
/// must not commit anything the new workers also process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_workers_that_keep_running_are_fenced() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 20).await;
    let sql = format!(
        "SET nodra.job_name = 'fence'; SET nodra.trigger = '20ms'; INSERT INTO delta('{}') SELECT id FROM delta('{}')",
        loc(&out),
        loc(&src)
    );
    run_once(&sql, 2).await.unwrap();
    // An old worker keeps running (streaming mode) ...
    let old = Arc::new(Pipeline::prepare(Job::parse(&sql, "x").unwrap()).await.unwrap());
    let (stop, rx) = watch::channel(false);
    let zombie = tokio::spawn(exec::run_worker(old, WorkerId { index: 0, count: 2 }, false, rx));
    tokio::time::sleep(Duration::from_millis(200)).await;
    // ... while the job is restarted with 3 workers.
    run_once(&sql, 3).await.unwrap();
    // New data; some of its files belong to the old worker (file assignment is by hash).
    for k in 0..6 {
        append(&src, events(200 + k * 10..210 + k * 10)).await;
    }
    let res = tokio::time::timeout(Duration::from_secs(60), zombie).await.expect("old worker never stopped").unwrap();
    let _ = stop.send(true);
    let err = res.unwrap_err().to_string();
    assert!(err.contains("restarted with 3 workers"), "{err}");
    run_once(&sql, 3).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
}
