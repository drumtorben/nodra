//! Regression (third review): partition-scoped merges must compare typed partition values, not
//! the raw strings. The original problem:
//!
//! `merge::merge` collects the partition values of the partials files added since the last
//! merge (raw strings from the commit JSON), then keeps the partials files and removes the sink
//! files whose raw partition values are in that set. A TIMESTAMP partition value is spelled
//! `2025-10-09 08:53:20.000000` by delta-rs and `2025-10-09 08:53:20` by Spark. Once a table
//! maintenance job (Spark/Databricks OPTIMIZE) has rewritten the partials table or the sink,
//! equal partitions no longer compare equal (the class-4 pairing was fixed for exactly this in
//! the second round, F5, with typed keys; the merge scope was not):
//!
//! * partials compacted: the compacted (old) partial states of a changed partition are left out
//!   of the merge, which then replaces the sink partition with the new states only;
//! * sink compacted: the compacted sink files of a changed partition are not removed, so the
//!   partition holds both the old and the recomputed rows.
use crate::support::planner as support;

use std::path::Path;

use support::*;

/// Simulate a Spark OPTIMIZE of `table`: compact it (`dataChange = false`), then respell the
/// TIMESTAMP partition values of the compaction commit the way Spark writes them.
async fn spark_optimize(table: &Path) {
    assert!(compact(table).await);
    let v = nodra::delta::version(&nodra::delta::open(&loc(table)).await.unwrap()).unwrap();
    let commit = table.join("_delta_log").join(format!("{v:020}.json"));
    let text = std::fs::read_to_string(&commit).unwrap();
    let respelled = text.replace(".000000\"", "\"");
    assert_ne!(text, respelled, "no timestamp partition values in {}", commit.display());
    std::fs::write(&commit, respelled).unwrap();
}

async fn scoped_after_optimize(name: &str, optimize_partials: bool) -> (Outcome, Outcome) {
    let dir = tempfile::tempdir().unwrap();
    let t = dir.path().join("t");
    typed_table_chunks(&t, 0..2).await;
    let out = dir.path().join(name);
    let sql = format!(
        "SET nodra.job_name = '{name}'; INSERT INTO delta('{}', partition_by => 'ts') \
         SELECT ts, count(*) AS k, sum(id) AS s FROM delta('{}') GROUP BY ts",
        loc(&out),
        loc(&t)
    );
    let first = outcome(&sql, 2, vec![]).await;
    if optimize_partials {
        spark_optimize(&dir.path().join(format!("{name}__nodra_partials"))).await;
    } else {
        spark_optimize(&out).await;
    }
    typed_table_chunks(&t, 2..4).await; // new rows in every existing `ts` partition
    let second = outcome(&sql, 2, vec![]).await;
    (first, second)
}

/// Expected 4 rows (3 timestamps + NULL) with the counts of all 48 source rows; the merge
/// recomputes the changed partitions from the new partial states only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_merge_after_partials_were_optimized_by_spark() {
    let (first, second) = scoped_after_optimize("spell_partials", true).await;
    println!("OUTCOME run 1: {first:?}\nOUTCOME run 2: {second:?}");
    assert!(matches!(first, Outcome::Matches { class: 3, .. }), "{first:?}");
    assert!(second.correct(), "{second:?}");
}

/// Expected 4 rows; the changed partitions keep their compacted old rows next to the new ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_merge_after_sink_was_optimized_by_spark() {
    let (first, second) = scoped_after_optimize("spell_sink", false).await;
    println!("OUTCOME run 1: {first:?}\nOUTCOME run 2: {second:?}");
    assert!(matches!(first, Outcome::Matches { class: 3, .. }), "{first:?}");
    assert!(second.correct(), "{second:?}");
}
