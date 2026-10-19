//! Exactly-once under concurrent replicas of the same worker (zombies, rolling rescales of
//! stream jobs). A FIFO used as a `jsonl_stream` partition file pauses the worker
//! deterministically inside a batch (after it read its progress, before it commits), so the
//! test can commit as a "zombie" replica in that window.
use crate::common;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use common::*;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use deltalake_core::kernel::{Action, Transaction};
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use nodra::delta::{self, CommitOutcome};
use nodra::exec::{self, Pipeline, WorkerId};
use nodra::job::Job;
use tokio::sync::watch;

fn mkfifo(p: &Path) {
    assert!(std::process::Command::new("mkfifo").arg(p).status().unwrap().success());
}

/// What a zombie replica that processed `ids` of a stream partition would commit.
async fn zombie_commit(out: &Path, ids: Vec<i64>, key: &str, next_offset: i64) {
    let mut t = delta::open(&loc(out)).await.unwrap();
    let schema = delta::arrow_schema(&t).unwrap();
    let batch = RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(ids)) as ArrayRef]).unwrap();
    let mut w = RecordBatchWriter::for_table(&t).unwrap();
    w.write(batch).await.unwrap();
    let actions: Vec<Action> = w.flush().await.unwrap().into_iter().map(Action::Add).collect();
    match delta::commit(&mut t, actions, vec![Transaction::new(key, next_offset)], delta::append_op(&[]), 3)
        .await
        .unwrap()
    {
        CommitOutcome::Committed(_) => {}
        other => panic!("zombie commit: {other:?}"),
    }
}

fn count_id(rows: &[String], id: i64) -> usize {
    rows.iter().filter(|r| r.as_str() == id.to_string()).count()
}

/// The worker's batch for partition 0 races with a zombie's commit for partition 1. delta-rs
/// resolves the race (disjoint app ids) and the post-commit snapshot includes the zombie's
/// commit; the worker must pick up the zombie's p1 progress from the commits in between instead
/// of re-reading partition 1 from offset 0 (the conflict check cannot see that commit any more).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn raced_commit_updates_cached_progress() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    append_jsonl(&stream, 0, &clicks(0..5).0);
    let sql = format!(
        "SET nodra.job_name = 'stale'; SET nodra.max_records_per_partition = '3'; \
         INSERT INTO delta('{}') SELECT id FROM jsonl_stream('{}', schema => 'id BIGINT')",
        loc(&out),
        loc(&stream)
    );
    run_once(&sql, 1).await.unwrap(); // p0 offsets 0..5 committed
    append_jsonl(&stream, 0, &clicks(5..9).0); // 4 new messages: two batches with max 3

    let fifo = stream.join("1.jsonl");
    mkfifo(&fifo);
    let p = Arc::new(Pipeline::prepare(Job::parse(&sql, "x").unwrap()).await.unwrap());
    let p1_key = p.stream_progress_key(0, 1);
    let (_tx, rx) = watch::channel(false);
    let worker = tokio::spawn(exec::run_worker(p.clone(), WorkerId { index: 0, count: 1 }, true, rx));

    // Batch 1 of the worker: it has fetched p0 5..8 and now reads p1 (blocks on the FIFO).
    let writer = {
        let fifo = fifo.clone();
        tokio::time::timeout(
            Duration::from_secs(60),
            tokio::task::spawn_blocking(move || std::fs::OpenOptions::new().write(true).open(fifo).unwrap()),
        )
        .await
        .expect("worker never read partition 1")
        .unwrap()
    };
    // p1 receives 3 messages (the worker keeps reading the empty FIFO it already opened) ...
    let tmp = dir.path().join("p1.tmp");
    std::fs::write(&tmp, clicks(100..103).0.join("\n") + "\n").unwrap();
    std::fs::rename(&tmp, &fifo).unwrap();
    // ... and a zombie replica processes them and commits p1 -> 3.
    zombie_commit(&out, vec![100, 101, 102], &p1_key, 3).await;
    drop(writer); // worker sees p1 as empty, commits p0 -> 8 (races with the zombie commit)

    tokio::time::timeout(Duration::from_secs(120), worker).await.expect("worker hung").unwrap().unwrap();

    let rows = table_rows(&loc(&out), &["id".to_string()]).await;
    let dup: Vec<i64> = [100, 101, 102].into_iter().filter(|i| count_id(&rows, *i) != 1).collect();
    assert!(dup.is_empty(), "p1 messages {dup:?} written more than once; sink has {} rows, expected 12", rows.len());
    assert_eq!(rows.len(), 12);
}

/// The worker reads "no target table" (so no progress), processes its input, and only then
/// creates/opens the target. Meanwhile a zombie created the table and committed the same
/// progress. The worker must re-verify its progress against the table it opened and abandon
/// the batch, instead of committing the input a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn first_batch_reverifies_progress_on_a_concurrently_created_target() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    append_jsonl(&stream, 0, &clicks(0..3).0);
    let fifo = stream.join("1.jsonl");
    mkfifo(&fifo);
    let sql = format!(
        "SET nodra.job_name = 'create'; \
         INSERT INTO delta('{}') SELECT id FROM jsonl_stream('{}', schema => 'id BIGINT')",
        loc(&out),
        loc(&stream)
    );
    let p = Arc::new(Pipeline::prepare(Job::parse(&sql, "x").unwrap()).await.unwrap());
    let p0_key = p.stream_progress_key(0, 0);
    let (_tx, rx) = watch::channel(false);
    let worker = tokio::spawn(exec::run_worker(p.clone(), WorkerId { index: 0, count: 1 }, true, rx));

    let writer = {
        let fifo = fifo.clone();
        tokio::time::timeout(
            Duration::from_secs(60),
            tokio::task::spawn_blocking(move || std::fs::OpenOptions::new().write(true).open(fifo).unwrap()),
        )
        .await
        .expect("worker never read partition 1")
        .unwrap()
    };
    // Zombie: creates the sink and commits p0 offsets 0..3.
    delta::create_or_open(&loc(&out), &p.sink_schema, &[]).await.unwrap();
    zombie_commit(&out, vec![0, 1, 2], &p0_key, 3).await;
    let tmp = dir.path().join("p1.tmp");
    std::fs::write(&tmp, "").unwrap();
    std::fs::rename(&tmp, &fifo).unwrap();
    drop(writer);

    tokio::time::timeout(Duration::from_secs(120), worker).await.expect("worker hung").unwrap().unwrap();
    let rows = table_rows(&loc(&out), &["id".to_string()]).await;
    assert_eq!(rows.len(), 3, "offsets 0..3 of p0 committed twice: {rows:?}");
}
