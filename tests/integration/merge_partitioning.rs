//! Regression (third review): `partial::merge_plan` must reproduce the split aggregate's output
//! partitioning and ordering for the operators above it. The original problem:
//!
//! `fit` always hash-partitions the restored states on *all* group columns. DataFusion's subset
//! satisfaction (`datafusion.optimizer.subset_repartition_threshold`, default 4) lets a
//! `SinglePartitioned` split aggregate over `(cid, day)` run directly on input partitioned by
//! `Hash([cid])`; its output then stays `Hash([cid])`, and operators above (an outer aggregate
//! or a partitioned join on `cid`, a window `PARTITION BY cid`) are planned without a
//! repartition of their own. In the merge plan the split becomes `FinalPartitioned` over
//! `Hash([cid, day])`, so the rows of one `cid` are spread over several partitions: the outer
//! `SinglePartitioned` aggregate emits a `cid` once per partition, a partitioned join misses
//! matches, a window fails DataFusion's ordering assertion.
//!
//! The partitioning of the split's input comes from a join below it whose output is hash
//! partitioned: with default settings, a join with an aggregated broadcast subquery (the merge
//! plan builds the hash table on the empty placeholder of the driving source and probes the
//! subquery's `Hash([cid])` output); with `prefer_hash_join = false` (sort-merge join) or a
//! zero `hash_join_single_partition_threshold`, any equi-join below. `target_partitions` is
//! pinned to 4 (the default is the CPU count, so this is the default on 4+ cores).
use crate::support::planner as support;

use support::*;

async fn run_cases(cases: &[(&str, &str, &str)]) -> Vec<String> {
    run_owned(cases.iter().map(|(a, b, c)| (a.to_string(), b.to_string(), c.to_string())).collect()).await
}

async fn run_owned(cases: Vec<(String, String, String)>) -> Vec<String> {
    let dir = tempfile::tempdir().unwrap();
    let (e, d) = (dir.path().join("e"), dir.path().join("d"));
    events_table(&e, 4, 30).await;
    dim_table(&d).await;
    let mut bad = Vec::new();
    for (name, set, body) in &cases {
        let sql = format!(
            "SET datafusion.execution.target_partitions = '4'; {set} INSERT INTO delta('{}') {}",
            loc(&dir.path().join(name)),
            body.replace("{E}", &format!("delta('{}')", loc(&e)))
                .replace("{D}", &format!("delta('{}')", loc(&d)))
        );
        let o = outcome(&sql, 3, vec![]).await;
        println!("OUTCOME {name}: {o:?}");
        if !o.correct() {
            bad.push(format!("{name}: {o:?}"));
        }
    }
    bad
}

/// Default settings. Nested aggregate: expected 6 rows (one per `cid`), got 12 (each `cid`
/// twice, with partial `max`/`count` values). Window: 18 rows, but `tot` is a per-partition
/// partial sum (e.g. 12 or 6 instead of 18).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_keeps_subset_partitioning_with_default_settings() {
    let split = "SELECT dd.cid, e.day, count(*) AS n FROM {E} e \
                 JOIN (SELECT cid, max(brand) AS b FROM {D} GROUP BY cid) dd ON e.cid = dd.cid GROUP BY dd.cid, e.day";
    let bad = run_cases(&[
        (
            "nested_over_broadcast_aggregate",
            "",
            &format!("SELECT cid, max(n) AS m, count(*) AS k FROM ({split}) t GROUP BY cid"),
        ),
        (
            "window_over_broadcast_aggregate",
            "",
            &format!("SELECT cid, day, n, sum(n) OVER (PARTITION BY cid) AS tot FROM ({split}) t"),
        ),
    ])
    .await;
    assert!(bad.is_empty(), "{bad:#?}");
}

/// Join strategies chosen with documented `SET datafusion.…` settings.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_keeps_subset_partitioning_with_partitioned_joins() {
    let smj = "SET datafusion.optimizer.prefer_hash_join = false;";
    let phj = "SET datafusion.optimizer.hash_join_single_partition_threshold = '0'; \
               SET datafusion.optimizer.hash_join_single_partition_threshold_rows = '0';";
    let nested =
        "SELECT cid, max(n) AS m, count(*) AS k FROM (SELECT e.cid, e.day, count(*) AS n FROM {E} e \
                  JOIN {D} d ON e.cid = d.cid GROUP BY e.cid, e.day) t GROUP BY cid";
    let bad = run_cases(&[
        // 12 rows instead of 6.
        ("smj_nested", smj, nested),
        ("partitioned_hash_join_nested", phj, nested),
        // 5 rows instead of 18: the partitioned join above misses matches.
        (
            "smj_join_above",
            smj,
            "SELECT t.cid, t.day, t.n, d2.brand FROM (SELECT e.cid, e.day, count(*) AS n FROM {E} e \
             JOIN {D} d ON e.cid = d.cid GROUP BY e.cid, e.day) t JOIN {D} d2 ON t.cid = d2.cid",
        ),
        // Merge fails: "All partition by columns should have an ordering" (the window relied on
        // the sort order of the sort-merge join below the split).
        (
            "smj_window",
            smj,
            "SELECT cid, day, n, sum(n) OVER (PARTITION BY cid) AS tot FROM (SELECT e.cid, e.day, count(*) AS n \
             FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY e.cid, e.day) t",
        ),
    ])
    .await;
    assert!(bad.is_empty(), "{bad:#?}");
}
