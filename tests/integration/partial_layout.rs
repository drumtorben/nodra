//! Regression (third review): the class-3 partial-state layout across edits of the query
//! between runs (refused since the partials table records its aggregation). The original problem:
//!
//! The canonical layout identifies *group* columns by name but in the order of the logical
//! aggregate, and the accumulator states purely by position (`s<k>`). The partials table keeps
//! states written by earlier runs, and nothing records which aggregate/group key a column held,
//! so a semantically neutral edit of a running class-3 job (reordering the SELECT list, which
//! `planner_shapes::by_name_writes_after_column_reorder` treats as supported, or reordering the
//! GROUP BY list) makes the merge read old states into the wrong accumulators whenever the
//! types happen to agree.
use crate::support::planner as support;

use support::*;

async fn two_runs(name: &str, v1: &str, v2: &str) -> (Outcome, Outcome) {
    let dir = tempfile::tempdir().unwrap();
    let e = dir.path().join("e");
    events_table(&e, 2, 30).await;
    let out = dir.path().join(name);
    let mk = |body: &str| {
        format!(
            "SET nodra.job_name = '{name}'; INSERT INTO delta('{}') {}",
            loc(&out),
            body.replace("{E}", &format!("delta('{}')", loc(&e)))
        )
    };
    let first = outcome(&mk(v1), 2, vec![]).await;
    append(&e, events(1000..1030)).await;
    let second = outcome(&mk(v2), 2, vec![]).await;
    (first, second)
}

/// Run 1 `count(*) AS n, sum(id) AS s`, run 2 the same aggregates listed the other way round.
/// Both states are Int64, so the old partial states (`s1` = count, `s2` = sum) are merged into
/// the swapped accumulators: every group's `n` and `s` are wrong after run 2.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reordered_aggregates_between_runs() {
    let (first, second) = two_runs(
        "reorder_aggs",
        "SELECT cid, count(*) AS n, sum(id) AS s FROM {E} GROUP BY cid",
        "SELECT cid, sum(id) AS s, count(*) AS n FROM {E} GROUP BY cid",
    )
    .await;
    println!("OUTCOME run 1: {first:?}\nOUTCOME run 2: {second:?}");
    assert!(matches!(first, Outcome::Matches { .. }), "{first:?}");
    // Refusing the changed layout would be fine; silently merging it is not.
    assert!(second.correct() || matches!(second, Outcome::Failed(_)), "{second:?}");
}

/// Run 1 `GROUP BY user, c`, run 2 `GROUP BY c, user` (same query semantics). Both keys are
/// strings, so old states are read with `user` and `c` swapped: the merge produces groups such
/// as (user = '3', c = 'u17') next to the right ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reordered_group_by_between_runs() {
    let (first, second) = two_runs(
        "reorder_keys",
        "SELECT user, CAST(cid AS VARCHAR) AS c, count(*) AS n FROM {E} GROUP BY user, CAST(cid AS VARCHAR)",
        "SELECT user, CAST(cid AS VARCHAR) AS c, count(*) AS n FROM {E} GROUP BY CAST(cid AS VARCHAR), user",
    )
    .await;
    println!("OUTCOME run 1: {first:?}\nOUTCOME run 2: {second:?}");
    assert!(matches!(first, Outcome::Matches { .. }), "{first:?}");
    assert!(second.correct() || matches!(second, Outcome::Failed(_)), "{second:?}");
}
