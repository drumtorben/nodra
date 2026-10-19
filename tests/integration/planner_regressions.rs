//! Regression tests from the second planner review: queries that once were accepted but gave
//! wrong results (compared with a single-node DataFusion run), or crashed at run time after
//! `nodra plan` accepted them. Each must give the right result or be rejected up front.
use crate::support::planner as support;

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use nodra::delta;
use support::*;

// ------------------------------------------------------------------------------------------ F1
/// An uncorrelated scalar subquery below the split aggregate. DataFusion 55 plans it as a
/// `ScalarSubqueryExec` at the *root* of the physical plan; `partial::worker_plan` cuts out the
/// split aggregate without that wrapper, so the subquery never runs on workers:
/// "ScalarSubqueryExpr evaluated before the subquery was executed". `nodra plan` accepts it
/// because it validates the full physical plan, not the worker plan.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scalar_subquery_below_class3_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let (e, d) = (dir.path().join("e"), dir.path().join("d"));
    events_table(&e, 2, 30).await;
    dim_table(&d).await;
    let mut bad = Vec::new();
    for (name, body) in [
        ("where", "SELECT cid, count(*) AS n FROM {E} WHERE cid > (SELECT avg(cid) FROM {D}) GROUP BY cid"),
        (
            "agg_filter",
            "SELECT day, count(*) FILTER (WHERE cid > (SELECT min(cid) FROM {D})) AS n FROM {E} GROUP BY day",
        ),
        ("in_aggregate_arg", "SELECT day, sum(v * (SELECT count(*) FROM {D})) AS s FROM {E} GROUP BY day"),
    ] {
        let sql = format!(
            "INSERT INTO delta('{}') {}",
            loc(&dir.path().join(name)),
            body.replace("{E}", &format!("delta('{}')", loc(&e)))
                .replace("{D}", &format!("delta('{}')", loc(&d)))
        );
        let o = outcome(&sql, 2, vec![]).await;
        println!("OUTCOME f1/{name}: {o:?}");
        // DataFusion 55 can only run scalar subqueries in a filter or projection as joins; inside
        // aggregates the job must fail at plan time, not on the workers.
        let at_plan_time =
            matches!(&o, Outcome::Failed(e) if e.starts_with("prepare: job error: cannot plan query"));
        if !(o.correct() || (name != "where" && at_plan_time)) {
            bad.push(format!("{name}: {o:?}"));
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

// ------------------------------------------------------------------------------------------ F2
/// A partitioned hash join *above* the split aggregate, on all group keys but in another order.
/// On workers the driving scan has unknown statistics, so DataFusion picks a partitioned join and
/// `EnforceDistribution::reorder_aggregation_keys` rewrites the Partial aggregate to group by
/// `(cid, user)`. The merge step plans the same query over an empty placeholder (exact 0 rows),
/// picks `CollectLeft`, keeps `(user, cid)`, and reads the positional state columns `s0, s1`
/// swapped: a cast error when the key types differ, silently wrong groups when they agree.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn join_above_aggregate_on_permuted_group_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (e, x) = (dir.path().join("e"), dir.path().join("x"));
    events_table(&e, 3, 40).await;
    cid_user_table(&x, 97).await;
    let mut bad = Vec::new();
    for (name, body) in [
        // Different key types: the merge fails with "Cannot cast string 'u76' to value of Int64 type".
        (
            "typed",
            "SELECT t.user, t.cid, t.n, x.label FROM (SELECT user, cid, count(*) AS n FROM {E} GROUP BY user, cid) t \
             JOIN {X} x ON t.cid = x.cid AND t.user = x.user",
        ),
        // Same key types: the merge swaps user and cid, the join matches nothing (0 of 120 rows).
        (
            "strings",
            "SELECT t.user, t.c, t.n FROM (SELECT user, CAST(cid AS VARCHAR) AS c, count(*) AS n FROM {E} GROUP BY user, CAST(cid AS VARCHAR)) t \
             JOIN {X} x ON t.c = CAST(x.cid AS VARCHAR) AND t.user = x.user",
        ),
    ] {
        // target_partitions defaults to the CPU count; pinned so the test does not depend on it.
        let sql = format!(
            "SET datafusion.execution.target_partitions = '4'; INSERT INTO delta('{}') {}",
            loc(&dir.path().join(name)),
            body.replace("{E}", &format!("delta('{}')", loc(&e))).replace("{X}", &format!("delta('{}')", loc(&x)))
        );
        let o = outcome(&sql, 2, vec![]).await;
        println!("OUTCOME f2/{name}: {o:?}");
        if !o.correct() {
            bad.push(format!("{name}: {o:?}"));
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

// ------------------------------------------------------------------------------------------ F3
/// The same table under two spellings (`/x/events` and `/x/./events`; likewise `file:///x/events`
/// or a relative path). `SourceSpec::key()` compares the raw strings, so the same-table rejection
/// does not fire: the second call is broadcast. The first run is right; an incremental run joins
/// the new rows of `a` with all of `b`, but never the new rows of `b` with the old rows of `a`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_table_under_another_spelling_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let e = dir.path().join("events");
    events_table(&e, 2, 30).await;
    let sql = format!(
        "INSERT INTO delta('{}') SELECT a.id AS a_id, b.id AS b_id FROM delta('{}') a JOIN delta('{}/./events') b \
         ON a.cid = b.cid AND a.id < b.id",
        loc(&dir.path().join("o")),
        loc(&e),
        loc(dir.path())
    );
    let o = outcome(&sql, 2, vec![]).await;
    println!("OUTCOME f3 run 1: {o:?}");
    if matches!(o, Outcome::Rejected(_)) {
        return;
    }
    append(&e, events(60..90)).await;
    run_once(&sql, 2).await.unwrap();
    let o = compare(&sql, 2, &[]).await;
    println!("OUTCOME f3 run 2: {o:?}");
    assert!(o.correct(), "{o:?}");
}

// ------------------------------------------------------------------------------------------ F4
/// The job class changes between runs. Run 1: `b` fits under the broadcast threshold, class 2
/// with `a` driving. `b` then grows past the threshold (still smaller than `a`), and run 2 plans
/// class 4: class-4 progress keys (`part/…`) do not exist yet, so every partition is joined again
/// and appended to the sink next to the class-2 output (the same happens from class 3 to 4, and
/// from 4 to 2/3 when `nodra.broadcast_threshold` is raised). `previous_driving` only looks at
/// per-file progress keys and does not stop the class change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class_change_between_runs_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    events_table(&a, 4, 30).await;
    let days: Vec<String> = (10..13).map(|d| format!("2026-10-{d}")).collect();
    create_table(&b, &spend(&days, 1).schema(), &["day"]).await;
    append(&b, spend(&days, 7)).await;
    let threshold = table_size(&b).await + 200;
    let sql = format!(
        "SET nodra.broadcast_threshold = '{threshold}'; INSERT INTO delta('{}') \
         SELECT a.id, a.day, b.w FROM delta('{}') a JOIN delta('{}') b ON a.day = b.day AND a.cid = b.cid",
        loc(&dir.path().join("o")),
        loc(&a),
        loc(&b)
    );
    let o = outcome(&sql, 2, vec![]).await;
    println!("OUTCOME f4 run 1: {o:?}");
    assert!(matches!(o, Outcome::Matches { class: 2, .. }), "{o:?}");
    append(&b, spend(&days, 3)).await;
    assert!(table_size(&b).await > threshold && table_size(&b).await < table_size(&a).await);
    match run_once(&sql, 2).await {
        // Refusing the class change is a correct outcome.
        Err(e) => println!("f4 run 2 refused: {e}"),
        Ok(p) => {
            let o = compare(&sql, p.plan.class.number(), &[]).await;
            println!("OUTCOME f4 run 2: {o:?}");
            assert!(o.correct(), "{o:?}");
        }
    }
}

// ------------------------------------------------------------------------------------------ F5
/// Class 4 pairs partitions of the two tables by their raw partition-value *strings*
/// (`DataFile::partition_key`). Delta writers spell typed values differently (Spark writes
/// `2025-10-09 08:53:20` for a TIMESTAMP partition, delta-rs `2025-10-09 08:53:20.000000`), so
/// equal partitions never meet: each looks present in only one table, stays pending forever, and
/// the join produces no rows (single node: 40 rows). The typed values are equal, and
/// `copartition_columns` already checks that the partition columns have the same type.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class4_pairs_partitions_by_typed_values() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
    ]));
    for t in [&a, &b] {
        create_table(t, &schema, &["ts"]).await;
        let ids: Vec<i64> = (0..40).collect();
        let ts: Vec<i64> = ids.iter().map(|i| 1_760_000_000_000_000 + (i % 4) * 3_600_000_000).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            ],
        )
        .unwrap();
        append(t, batch).await;
    }
    // Rewrite b's partition values the way Spark spells timestamps (no fractional seconds).
    let commit = b.join("_delta_log").join("00000000000000000001.json");
    let text = std::fs::read_to_string(&commit).unwrap();
    let respelled = text.replace(".000000\"}", "\"}");
    assert_ne!(text, respelled);
    std::fs::write(&commit, respelled).unwrap();
    let pa = delta::active_files(&delta::open(&loc(&a)).await.unwrap()).unwrap()[0].partition_values.clone();
    let pb = delta::active_files(&delta::open(&loc(&b)).await.unwrap()).unwrap()[0].partition_values.clone();
    println!("partition values: a {pa:?}, b {pb:?}");
    let sql = format!(
        "SET nodra.broadcast_threshold = '1KB'; INSERT INTO delta('{}') SELECT a.id, a.ts FROM delta('{}') a \
         JOIN delta('{}') b ON a.ts = b.ts AND a.id = b.id",
        loc(&dir.path().join("o")),
        loc(&a),
        loc(&b)
    );
    let o = outcome(&sql, 2, vec![]).await;
    println!("OUTCOME f5: {o:?}");
    assert!(o.correct(), "{o:?}");
}

// ------------------------------------------------------------------------------------------ F6
/// Two sink partition columns derived from the same group key: `partials_partition_columns`
/// returns `[s0, s0]`, and creating the partials table fails at run time ("a Delta table needs
/// at least one non-partition column") although `nodra plan` accepted the job.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_sink_partitions_from_one_group_key() {
    let dir = tempfile::tempdir().unwrap();
    let e = dir.path().join("e");
    events_table(&e, 2, 30).await;
    let sql = format!(
        "INSERT INTO delta('{}', partition_by => 'day, day2') SELECT day, day AS day2, count(*) AS n FROM delta('{}') GROUP BY day",
        loc(&dir.path().join("o")),
        loc(&e)
    );
    let o = outcome(&sql, 2, vec![]).await;
    println!("OUTCOME f6: {o:?}");
    assert!(o.correct(), "{o:?}");
}

// ------------------------------------------------------------------------------------------ F7
/// Output columns whose names collide case-insensitively (`e.cid, d.cid`, or `id, ID`) pass
/// `nodra plan` (`types::delta_schema` only checks characters) and fail at the first batch when
/// the sink is created: "Duplicate field name (case-insensitive): 'cid'".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn duplicate_output_names_fail_at_plan_time() {
    let dir = tempfile::tempdir().unwrap();
    let (e, d) = (dir.path().join("e"), dir.path().join("d"));
    events_table(&e, 2, 30).await;
    dim_table(&d).await;
    let sql = format!(
        "INSERT INTO delta('{}') SELECT e.id, e.cid, d.cid FROM delta('{}') e LEFT JOIN delta('{}') d ON e.cid = d.cid",
        loc(&dir.path().join("o")),
        loc(&e),
        loc(&d)
    );
    let prepared = nodra::exec::Pipeline::prepare(nodra::job::Job::parse(&sql, "t").unwrap()).await;
    if prepared.is_err() {
        return; // rejected up front: fine
    }
    let run = run_once(&sql, 2).await;
    assert!(run.is_ok(), "accepted by the planner, then failed: {:?}", run.err());
}
