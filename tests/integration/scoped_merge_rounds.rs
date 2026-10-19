//! Scoped merges over several rounds and late workers around a compaction (verified in the
//! third review).
use crate::support::planner as support;

use std::sync::Arc;

use nodra::exec::{self, Pipeline};
use nodra::job::Job;
use support::*;
use tokio::sync::watch;

/// Partition-scoped merges with the canonical partial-state layout over three rounds of input:
/// sink partitions derived from a group key that is not the first one (`s1`), two sink
/// partition columns in the opposite order of the group keys, through an outer aggregate,
/// `count(DISTINCT …)`, `SELECT DISTINCT`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_merges_with_canonical_layout_over_rounds() {
    let dir = tempfile::tempdir().unwrap();
    let e = dir.path().join("e");
    events_table(&e, 2, 30).await;
    let jobs: Vec<String> = [
        ("k1", ", partition_by => 'day'", "SELECT cid, day, count(*) AS n, sum(v) AS s FROM {E} GROUP BY cid, day"),
        (
            "k1_distinct",
            ", partition_by => 'day'",
            "SELECT cid, day, count(DISTINCT user) AS u FROM {E} GROUP BY cid, day",
        ),
        (
            "two_parts_swapped",
            ", partition_by => 'day, b'",
            "SELECT cid % 3 AS b, day, count(*) AS n FROM {E} GROUP BY cid % 3, day",
        ),
        (
            "nested",
            ", partition_by => 'd'",
            "SELECT d, max(n) AS m FROM (SELECT cid, day AS d, count(*) AS n FROM {E} GROUP BY cid, day) t GROUP BY d",
        ),
        ("distinct_rows", ", partition_by => 'day'", "SELECT DISTINCT user, day FROM {E}"),
    ]
    .iter()
    .map(|(name, sink, body)| {
        format!(
            "SET datafusion.execution.target_partitions = '4'; INSERT INTO delta('{}'{sink}) {}",
            loc(&dir.path().join(name)),
            body.replace("{E}", &format!("delta('{}')", loc(&e)))
        )
    })
    .collect();
    let mut bad = Vec::new();
    for round in 0..3 {
        if round > 0 {
            append(&e, events(1000 * round..1000 * round + 7)).await;
        }
        for sql in &jobs {
            let o = outcome(sql, 3, vec![]).await;
            println!("OUTCOME round {round}: {o:?}");
            if !matches!(o, Outcome::Matches { class: 3, .. }) {
                bad.push(format!("round {round} {sql}: {o:?}"));
            }
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

/// `nodra.start = 'latest'` on a Delta source: a worker that starts later (after an append, an
/// OPTIMIZE and another append) starts at the version recorded by the first worker, not at its
/// own latest version, so nothing is skipped or repeated.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delta_start_latest_late_worker_around_a_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 3, 20).await;
    let sql = format!(
        "SET nodra.job_name = 'latestv'; SET nodra.start = 'latest'; \
         INSERT INTO delta('{}') SELECT id, cid FROM delta('{}')",
        loc(&out),
        loc(&src)
    );
    let run_some = |idx: Vec<u32>| {
        let sql = sql.clone();
        async move {
            let p = Arc::new(Pipeline::prepare(Job::parse(&sql, "x").unwrap()).await.unwrap());
            let (_tx, rx) = watch::channel(false);
            exec::run(p, idx, 2, true, rx).await.unwrap();
        }
    };
    run_some(vec![0]).await; // records start = version 3
    append(&src, events(100..110)).await; // v4
    assert!(compact(&src).await); // v5, dataChange = false
    append(&src, events(200..210)).await; // v6
    run_some(vec![1]).await; // starts at 3, not 6
    run_once(&sql, 2).await.unwrap();
    let ids = table_rows(&loc(&out), &["id".to_string()]).await;
    let mut want: Vec<String> = (100..110).chain(200..210).map(|i: i64| i.to_string()).collect();
    want.sort();
    assert_eq!(ids, want);
}
