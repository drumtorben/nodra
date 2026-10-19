//! Where workers start: the shared initial-snapshot version of Delta-driven jobs, snapshot
//! progress under zombie replicas, and `nodra.start = 'latest'` for streams.
use crate::common;

use std::sync::Arc;

use common::*;
use nodra::delta;
use nodra::exec::{self, Pipeline, WorkerId};
use nodra::job::Job;
use tokio::sync::watch;

async fn run_some(sql: &str, indices: &[u32], count: u32) -> nodra::Result<()> {
    let p = Arc::new(Pipeline::prepare(Job::parse(sql, "test")?).await?);
    let (_tx, rx) = watch::channel(false);
    exec::run(p, indices.to_vec(), count, true, rx).await
}

/// Each worker takes its initial snapshot at the source version that is latest when *it*
/// starts. Worker 0 of 2 snapshots version 3; then the source is compacted (OPTIMIZE,
/// `dataChange = false`); worker 1 starts later (pods start at different times, or it crashed
/// before its first commit) and snapshots version 4, which only contains the compacted files.
/// Compacted files owned by worker 1 hold rows worker 0 already wrote (duplicates); compacted
/// files owned by worker 0 are processed by nobody, because worker 0 skips the
/// `dataChange = false` commit (worker 1's original rows are lost).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_share_one_snapshot_version_around_a_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 3, 20).await; // 3 commits x 3 day partitions = 9 files
    let sql = format!("SET nodra.job_name = 'snapv'; INSERT INTO delta('{}') SELECT id, cid FROM delta('{}')", loc(&out), loc(&src));
    run_some(&sql, &[0], 2).await.unwrap(); // worker 0: snapshot at version 3
    assert!(compact(&src).await); // version 4: 3 compacted files, dataChange = false
    run_some(&sql, &[1], 2).await.unwrap(); // worker 1: snapshot at version 4
    run_once(&sql, 2).await.unwrap();
    assert_eq!(assert_matches_reference(&sql, &[]).await, 60);
}

/// Two replicas of worker 0 (a zombie from before a config change and its replacement) both
/// read "no progress" before either commits. With different chunk sizes, the replacement
/// processes the whole snapshot in one batch and commits only `w0of1` (last chunk), while the
/// zombie processes a first chunk and commits only `w0of1/snapshot`: the two commits write
/// disjoint progress keys, so delta-rs' conflict check lets both through. (Timing-dependent:
/// both replicas must read the target before either commits; several rounds are tried.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zombie_with_other_chunk_size_during_initial_snapshot_conflicts() {
    for round in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let (src, out) = (dir.path().join("events"), dir.path().join("out"));
        events_table(&src, 6, 25).await; // 18 files
        let base = format!("INSERT INTO delta('{}') SELECT id, v FROM delta('{}')", loc(&out), loc(&src));
        let name = format!("snapzombie{round}");
        let big = Arc::new(
            Pipeline::prepare(Job::parse(&format!("SET nodra.job_name = '{name}'; {base}"), "x").unwrap()).await.unwrap(),
        );
        let small = Arc::new(
            Pipeline::prepare(
                Job::parse(&format!("SET nodra.job_name = '{name}'; SET nodra.max_files_per_batch = '2'; {base}"), "x").unwrap(),
            )
            .await
            .unwrap(),
        );
        // The target exists (as after an earlier, unrelated commit), without progress.
        delta::create_or_open(&loc(&out), &big.sink_schema, &[]).await.unwrap();
        let (_tx, rx) = watch::channel(false);
        let id = WorkerId { index: 0, count: 1 };
        let a = tokio::spawn(exec::run_worker(big.clone(), id, true, rx.clone()));
        let b = tokio::spawn(exec::run_worker(small.clone(), id, true, rx.clone()));
        let (ra, rb) = (a.await.unwrap(), b.await.unwrap());
        eprintln!("round {round}: {ra:?} {rb:?}");
        let sql = format!("SET nodra.job_name = '{name}'; {base}");
        assert_eq!(assert_matches_reference(&sql, &[]).await, 150, "round {round}");
    }
}

/// `nodra.start = 'latest'` must only apply when the job starts. Worker 1 of 2 owned no
/// partition while the topic had one partition; when partition 1 appears, worker 1 sees "none
/// of my partitions has progress" and treats it as the job start, skipping the partition's
/// existing messages (docs: partitions that appear later start at their earliest offset). The
/// same happens when the topic is expanded and the job scaled out together (2 -> 4 partitions
/// and workers: workers 2 and 3 own only new partitions).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_latest_reads_a_new_partition_of_an_idle_worker() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    append_jsonl(&stream, 0, &clicks(0..10).0);
    let sql = format!(
        "SET nodra.start = 'latest'; INSERT INTO delta('{}') SELECT id FROM jsonl_stream('{}', schema => 'id BIGINT')",
        loc(&out),
        loc(&stream)
    );
    run_once(&sql, 2).await.unwrap(); // p0 starts at offset 10; worker 1 has no partition
    let (a, ba) = clicks(10..14);
    append_jsonl(&stream, 0, &a);
    let (b, bb) = clicks(100..105);
    append_jsonl(&stream, 1, &b); // a partition that appears after the job started
    run_once(&sql, 2).await.unwrap();
    let ids = table_rows(&loc(&out), &["id".to_string()]).await;
    let expected = render(&[ba.project(&[0]).unwrap(), bb.project(&[0]).unwrap()]);
    assert_eq!(ids, expected);
}

/// Rescaling a stream job while a new
/// partition appears on a worker that also owns partitions with progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_latest_new_partition_on_a_busy_worker() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    append_jsonl(&stream, 0, &clicks(0..10).0);
    append_jsonl(&stream, 1, &clicks(100..110).0);
    let sql = format!(
        "SET nodra.start = 'latest'; INSERT INTO delta('{}') SELECT id FROM jsonl_stream('{}', schema => 'id BIGINT')",
        loc(&out),
        loc(&stream)
    );
    run_once(&sql, 2).await.unwrap();
    let (c, bc) = clicks(200..203);
    append_jsonl(&stream, 2, &c); // p2 -> worker 0 (with p0)
    run_once(&sql, 2).await.unwrap();
    let ids = table_rows(&loc(&out), &["id".to_string()]).await;
    assert_eq!(ids, render(&[bc.project(&[0]).unwrap()]));
}
