//! Worker generations of Delta-driven jobs (`nodra::rescale`, `Worker::placement`,
//! `step_delta`): rescale chains, concurrent worker counts, crashes during a handover.
use crate::common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use nodra::delta;
use nodra::exec::{self, Pipeline};
use nodra::job::Job;
use tokio::sync::watch;

/// Run only the workers `indices` of `count` (until caught up).
async fn run_some(sql: &str, indices: &[u32], count: u32) -> nodra::Result<()> {
    let p = Arc::new(Pipeline::prepare(Job::parse(sql, "test")?).await?);
    let (_tx, rx) = watch::channel(false);
    exec::run(p, indices.to_vec(), count, true, rx).await
}

fn job(name: &str, out: &std::path::Path, src: &std::path::Path, class3: bool, pin: Option<i64>) -> String {
    let pin = pin.map(|v| format!(", version => {v}")).unwrap_or_default();
    if class3 {
        format!(
            "SET nodra.job_name = '{name}'; INSERT INTO delta('{}') \
             SELECT cid, count(*) AS n, sum(id) AS s FROM delta('{}'{pin}) GROUP BY cid",
            loc(out),
            loc(src)
        )
    } else {
        format!("SET nodra.job_name = '{name}'; INSERT INTO delta('{}') SELECT id, cid FROM delta('{}'{pin})", loc(out), loc(src))
    }
}

/// Rescale chain 2 -> 3 -> 2. Generation 0 stopped with worker 0 ahead of worker 1
/// (done = [6, 2]). Generation 1 (3 workers) only got to version 3 before it was stopped (here
/// via a pinned source version, deterministic; in production: stopped/rolled back while
/// catching up, every worker having committed progress as the docs require). Generation 2
/// takes over from generation 1 only: it starts at 3 and treats every file of versions 4..6 as
/// unprocessed, although generation 0's worker 0 already processed its share of them.
async fn chain(class3: bool) {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 20).await; // versions 0..=2
    let name = format!("chain{}", class3 as u8);
    let sql = job(&name, &out, &src, class3, None);
    run_once(&sql, 2).await.unwrap(); // generation 0: done = [2, 2]
    for k in 0..4 {
        append(&src, events(100 + k * 10..110 + k * 10)).await; // versions 3..=6
    }
    run_some(&sql, &[0], 2).await.unwrap(); // generation 0: done = [6, 2]
    // Generation 1 (3 workers) gets to version 3; every worker commits progress.
    run_once(&job(&name, &out, &src, class3, Some(3)), 3).await.unwrap();
    // Back to 2 workers: generation 2.
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescale_chain_skips_files_of_every_earlier_generation() {
    chain(false).await;
}

/// Class 3 (target = partials table): the duplicated partial states double-count in the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescale_chain_keeps_partial_states_exact() {
    chain(true).await;
}

/// Same chain, following the documented procedure literally: the 3-worker run is stopped as
/// soon as `nodra status` shows progress for every worker, then the job is started with 2
/// workers again. (Timing-dependent in how far generation 1 gets, but with one file per batch
/// and a large backlog it never catches up before the stop.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rescale_back_after_documented_stop() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 20).await;
    let sql = format!(
        "SET nodra.job_name = 'stopchain'; SET nodra.max_files_per_batch = '1'; SET nodra.trigger = '1h'; \
         INSERT INTO delta('{}') SELECT id, cid FROM delta('{}')",
        loc(&out),
        loc(&src)
    );
    run_once(&sql, 2).await.unwrap();
    for k in 0..15 {
        append(&src, events(1000 + k * 10..1010 + k * 10)).await;
    }
    run_some(&sql, &[0], 2).await.unwrap(); // worker 0 of 2 is 15 versions ahead of worker 1

    // Start 3 workers (streaming), stop them once every worker of generation 1 has progress.
    let p = Arc::new(Pipeline::prepare(Job::parse(&sql, "x").unwrap()).await.unwrap());
    let prefix = p.delta_source_prefix(0);
    let (stop, rx) = watch::channel(false);
    let run = tokio::spawn(exec::run(p.clone(), vec![0, 1, 2], 3, false, rx));
    let deadline = std::time::Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(t) = delta::try_open(&loc(&out)).await.unwrap() {
            let all = delta::txn_map(&t).await.unwrap();
            let gens = nodra::rescale::generations(&all, &prefix).unwrap();
            if gens.get(&1).is_some_and(|g| g.done.len() == 3) {
                break;
            }
        }
        assert!(std::time::Instant::now() < deadline, "generation 1 never committed");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let _ = stop.send(true);
    run.await.unwrap().unwrap();
    let status = nodra::status::render(&p).await.unwrap();
    eprintln!("{status}");

    run_once(&sql, 2).await.unwrap(); // generation 2
    assert_matches_reference(&sql, &[]).await;
}

/// Two runs with different new worker counts (3 and 4) start at the same time (e.g. an
/// operator changes `--workers` twice in a row, or a `--once` backfill overlaps the
/// deployment). Both place themselves in generation 1. The first commit of one of them fences
/// generation 0's keys; the other's first commit conflicts, but on the retry it keeps its cached
/// placement (generation 1 with its own count), sees generation 1 as "started", skips the fence
/// and commits — both runs process the same input. Generation 1 then holds keys of two worker
/// counts, which `rescale::generations` rejects from then on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_new_worker_counts_started_together_are_refused() {
    for round in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let (src, out) = (dir.path().join("events"), dir.path().join("out"));
        events_table(&src, 2, 20).await;
        let sql = format!(
            "SET nodra.job_name = 'twocounts'; INSERT INTO delta('{}') SELECT id, cid FROM delta('{}')",
            loc(&out),
            loc(&src)
        );
        run_once(&sql, 2).await.unwrap();
        for k in 0..20 {
            append(&src, events(1000 + k * 10..1010 + k * 10)).await;
        }
        let (s3, s4) = (sql.clone(), sql.clone());
        let a = tokio::spawn(async move { run_once(&s3, 3).await.map(|_| ()) });
        let b = tokio::spawn(async move { run_once(&s4, 4).await.map(|_| ()) });
        let (ra, rb) = (a.await.unwrap(), b.await.unwrap());
        eprintln!("round {round}: 3 workers -> {ra:?}, 4 workers -> {rb:?}");
        // Whatever the outcome, nothing may have been committed twice, and the job must still
        // be usable.
        let (expected, names) = reference(&sql, &[]).await;
        let got = table_rows(&loc(&out), &names).await;
        let p = Pipeline::prepare(Job::parse(&sql, "x").unwrap()).await.unwrap();
        let status = nodra::status::render(&p).await;
        let next = run_once(&sql, 3).await.map(|_| ());
        assert!(
            got == expected && status.is_ok() && next.is_ok(),
            "round {round}: sink has {} rows, expected {}; status: {:?}; next run: {next:?}",
            got.len(),
            expected.len(),
            status.map(|_| "ok")
        );
    }
}

/// A new-generation worker crashes after
/// writing its first batch's files but before committing, and is restarted.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_before_the_first_commit_of_a_generation() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 20).await;
    let sql = format!("SET nodra.job_name = 'gencrash'; INSERT INTO delta('{}') SELECT id, cid FROM delta('{}')", loc(&out), loc(&src));
    run_once(&sql, 2).await.unwrap();
    for k in 0..4 {
        append(&src, events(100 + k * 10..110 + k * 10)).await;
    }
    run_some(&sql, &[1], 2).await.unwrap(); // done = [2, 6]
    exec::testing::fail_before_commit("gencrash", 1);
    assert!(run_once(&sql, 3).await.is_err());
    run_once(&sql, 3).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
    append(&src, events(500..520)).await;
    run_once(&sql, 5).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
}
