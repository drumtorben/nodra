//! Differential tests of the planner: every query is either rejected, or its result with N
//! workers (and after incremental runs) equals a single-node DataFusion run of the same query.
//! Most cases come from an independent review of the planner's classification.
//!
//! Every case prints `OUTCOME <name>: …` (`-- --nocapture` shows them).
mod common;

use std::path::PathBuf;
use std::sync::Arc;

use common::*;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use nodra::exec::Pipeline;
use nodra::job::Job;
use nodra::NodraError;

// ------------------------------------------------------------------------------------------ helpers

#[derive(Debug, Clone)]
enum Outcome {
    Rejected(String),
    Failed(String),
    Matches { class: u8, rows: usize },
    Differs { class: u8, got: usize, expected: usize, missing: Vec<String>, unexpected: Vec<String> },
}

impl Outcome {
    fn correct(&self) -> bool {
        matches!(self, Outcome::Rejected(_) | Outcome::Matches { .. })
    }
}

struct Env {
    _dir: tempfile::TempDir,
    root: PathBuf,
    e: String,
    d: String,
}

/// events: 4 commits x 30 rows, 3 day partitions -> 12 files; dim: cid 0..5.
async fn env() -> Env {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path().to_path_buf();
    let (e, d) = (root.join("events"), root.join("dim"));
    events_table(&e, 4, 30).await;
    dim_table(&d).await;
    Env { e: loc(&e), d: loc(&d), root, _dir: dir }
}

impl Env {
    fn out(&self, name: &str) -> String {
        loc(&self.root.join(name))
    }
    /// `{E}` / `{D}` in `body` become the source calls; `sink_args` go into the sink call.
    fn sql(&self, name: &str, sink_args: &str, body: &str) -> String {
        let q = body.replace("{E}", &format!("delta('{}')", self.e)).replace("{D}", &format!("delta('{}')", self.d));
        format!("INSERT INTO delta('{}'{sink_args}) {q}", self.out(name))
    }
}

async fn prepare(sql: &str) -> nodra::Result<Pipeline> {
    Pipeline::prepare(Job::parse(sql, "test")?).await
}

async fn compare(sql: &str, class: u8) -> Outcome {
    let (expected, names) = reference(sql, &[]).await;
    let job = Job::parse(sql, "ref").unwrap();
    let got = table_rows(job.sink.uri(), &names).await;
    if got == expected {
        Outcome::Matches { class, rows: got.len() }
    } else {
        let e: std::collections::BTreeSet<_> = expected.iter().cloned().collect();
        let g: std::collections::BTreeSet<_> = got.iter().cloned().collect();
        Outcome::Differs {
            class,
            got: got.len(),
            expected: expected.len(),
            missing: e.difference(&g).take(4).cloned().collect(),
            unexpected: g.difference(&e).take(4).cloned().collect(),
        }
    }
}

/// Plan, run with `workers` workers, compare with the single-node reference.
async fn outcome(sql: &str, workers: u32) -> Outcome {
    let sql = sql.to_string();
    let h = tokio::spawn(async move {
        match prepare(&sql).await {
            Err(NodraError::Rejected(r)) => return Outcome::Rejected(r.to_string()),
            Err(e) => return Outcome::Failed(format!("prepare: {e}")),
            Ok(_) => {}
        }
        let p = match run_once(&sql, workers).await {
            Ok(p) => p,
            Err(e) => return Outcome::Failed(format!("run: {e}")),
        };
        compare(&sql, p.plan.class.number()).await
    });
    match h.await {
        Ok(o) => o,
        Err(e) => Outcome::Failed(format!("panic: {e}")),
    }
}

fn report(name: &str, o: &Outcome) {
    println!("OUTCOME {name}: {o:?}");
}

fn events_on_day(ids: &[i64], day: &str, v: impl Fn(i64) -> f64) -> RecordBatch {
    RecordBatch::try_new(
        events_schema(),
        vec![
            Arc::new(Int64Array::from(ids.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(ids.iter().map(|i| i % 7).collect::<Vec<_>>())),
            Arc::new(StringArray::from(ids.iter().map(|i| format!("u{}", i % 13)).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(ids.iter().map(|i| v(*i)).collect::<Vec<_>>())),
            Arc::new(StringArray::from(vec![day; ids.len()])),
        ],
    )
    .unwrap()
}

/// Events spread over `ndays` day partitions.
fn events_ndays(range: std::ops::Range<i64>, ndays: i64) -> RecordBatch {
    let b = events(range.clone());
    let days: Vec<String> = range.map(|i| format!("2026-11-{:02}", 1 + (i % ndays))).collect();
    let mut cols = b.columns().to_vec();
    cols[4] = Arc::new(StringArray::from(days));
    RecordBatch::try_new(b.schema(), cols).unwrap()
}

fn spend(ndays: i64, per_day: i64) -> RecordBatch {
    let (mut day, mut cid, mut w) = (Vec::new(), Vec::new(), Vec::new());
    for d in 0..ndays {
        for c in 0..per_day {
            day.push(format!("2026-11-{:02}", 1 + d));
            cid.push(c);
            w.push(c as f64 * 1.5 + d as f64);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("cid", DataType::Int64, true),
            Field::new("w", DataType::Float64, true),
            Field::new("day", DataType::Utf8, true),
        ])),
        vec![Arc::new(Int64Array::from(cid)) as ArrayRef, Arc::new(Float64Array::from(w)), Arc::new(StringArray::from(day))],
    )
    .unwrap()
}

fn dim_rows(cids: std::ops::Range<i64>, pad: usize) -> RecordBatch {
    let cid: Vec<i64> = cids.collect();
    // Incompressible padding (so Parquet size grows with the row count).
    let brand: Vec<String> = cid
        .iter()
        .map(|c| {
            let mut s = format!("brand-{}", c % 3);
            let mut x = (*c as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15) ^ 0xD1B5_4A32_D192_ED03;
            for _ in 0..pad {
                x ^= x << 13;
                x ^= x >> 7;
                x ^= x << 17;
                s.push(char::from(b'a' + (x % 26) as u8));
            }
            s
        })
        .collect();
    RecordBatch::try_new(dim_schema(), vec![Arc::new(Int64Array::from(cid)), Arc::new(StringArray::from(brand))]).unwrap()
}

// ------------------------------------------------------------- A. CTE: driving source read twice

/// A CTE that wraps the driving source is inlined twice: both scans would be split across
/// workers, so each worker would only join its own share. Must be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a1_cte_self_join_class1() {
    let env = env().await;
    let sql = env.sql(
        "a1",
        "",
        "WITH e AS (SELECT * FROM {E}) SELECT a.id AS a_id, b.id AS b_id FROM e a JOIN e b ON a.cid = b.cid AND a.id < b.id",
    );
    let o = outcome(&sql, 3).await;
    report("a1_cte_self_join_class1", &o);
    assert!(o.correct(), "{o:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a2_cte_self_join_then_aggregate_class3() {
    let env = env().await;
    let sql = env.sql(
        "a2",
        "",
        "WITH e AS (SELECT * FROM {E}) SELECT a.cid, count(*) AS pairs FROM e a JOIN e b ON a.cid = b.cid GROUP BY a.cid",
    );
    let o = outcome(&sql, 3).await;
    report("a2_cte_self_join_then_aggregate_class3", &o);
    assert!(o.correct(), "{o:?}");
}

/// Correlated scalar subquery over the driving source through a CTE (decorrelated into a join
/// with an aggregate over a second scan of the driving source).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a3_cte_correlated_scalar_subquery() {
    let env = env().await;
    let sql = env.sql(
        "a3",
        "",
        "WITH e AS (SELECT * FROM {E}) SELECT o.id, o.v FROM e o WHERE o.v > (SELECT avg(i.v) FROM e i WHERE i.cid = o.cid)",
    );
    let o = outcome(&sql, 3).await;
    report("a3_cte_correlated_scalar_subquery", &o);
    assert!(o.correct(), "{o:?}");
}

/// Same idea with a join against an aggregated CTE ("deviation from the per-key mean").
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a4_cte_join_with_own_aggregate() {
    let env = env().await;
    let sql = env.sql(
        "a4",
        "",
        "WITH e AS (SELECT * FROM {E}), m AS (SELECT cid, avg(v) AS av FROM e GROUP BY cid) \
         SELECT e.id, e.v - m.av AS dev FROM m JOIN e ON e.cid = m.cid",
    );
    let o = outcome(&sql, 3).await;
    report("a4_cte_join_with_own_aggregate", &o);
    assert!(o.correct(), "{o:?}");
}

/// UNION ALL whose first branch aggregates the driving source and whose second reads it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a5_cte_union_after_aggregate() {
    let env = env().await;
    let sql = env.sql(
        "a5",
        "",
        "WITH e AS (SELECT * FROM {E}) SELECT cid, count(*) AS n FROM e GROUP BY cid \
         UNION ALL SELECT cid, id AS n FROM e WHERE id < 5",
    );
    let o = outcome(&sql, 3).await;
    report("a5_cte_union_after_aggregate", &o);
    assert!(o.correct(), "{o:?}");
}

// ----------------------------------------------------------- B. ORDER BY ... LIMIT = Sort{fetch}

/// The optimizer folds `Limit` over `Sort` into `Sort { fetch }` and drops the Limit node: a
/// top-N over the driving source must still be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b1_order_by_limit_class1() {
    let env = env().await;
    let sql = env.sql("b1", "", "SELECT id, v FROM {E} ORDER BY v DESC LIMIT 5");
    let o = outcome(&sql, 3).await;
    report("b1_order_by_limit_class1", &o);
    assert!(o.correct(), "{o:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b2_order_by_limit_class2_join() {
    let env = env().await;
    let sql = env.sql("b2", "", "SELECT e.id, d.brand FROM {E} e JOIN {D} d ON e.cid = d.cid ORDER BY e.id LIMIT 3");
    let o = outcome(&sql, 3).await;
    report("b2_order_by_limit_class2_join", &o);
    assert!(o.correct(), "{o:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b3_top_n_subquery_below_aggregate() {
    let env = env().await;
    let sql = env.sql(
        "b3",
        "",
        "SELECT cid, count(*) AS n FROM (SELECT * FROM {E} ORDER BY v DESC LIMIT 10) t GROUP BY cid",
    );
    let o = outcome(&sql, 3).await;
    report("b3_top_n_subquery_below_aggregate", &o);
    assert!(o.correct(), "{o:?}");
}

/// A top-N above the aggregate needs all groups: the merge must not be partition-scoped, or a
/// later merge would apply the global top-N to the changed day alone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn b4_partition_scoped_merge_with_top_n() {
    let env = env().await;
    let sql = env.sql(
        "b4",
        ", partition_by => 'day'",
        "SELECT day, cid, count(*) AS n FROM {E} GROUP BY day, cid ORDER BY n DESC, day, cid LIMIT 4",
    );
    let p = prepare(&sql).await.unwrap();
    println!("b4 class {} merge scope {:?}", p.plan.class.number(), p.plan.merge_scope);
    let o1 = outcome(&sql, 3).await;
    report("b4 run1", &o1);
    // New rows only for 2026-10-11.
    let ids: Vec<i64> = (500..530).collect();
    append(std::path::Path::new(&env.e), events_on_day(&ids, "2026-10-11", |i| i as f64)).await;
    run_once(&sql, 3).await.unwrap();
    let o2 = compare(&sql, 3).await;
    report("b4_partition_scoped_merge_with_top_n (after incremental run)", &o2);
    assert!(o2.correct(), "{o2:?}");
}

// ----------------------------------------------- C. class 4: operators above the per-partition part

async fn class4_env() -> (tempfile::TempDir, String, String) {
    let dir = tempfile::tempdir().unwrap();
    let (a, b) = (dir.path().join("a"), dir.path().join("b"));
    create_table(&a, &events_schema(), &["day"]).await;
    append(&a, events_ndays(0..80, 8)).await;
    append(&a, events_ndays(80..160, 8)).await;
    let sb = spend(1, 1).schema();
    create_table(&b, &sb, &["day"]).await;
    append(&b, spend(8, 7)).await;
    (dir, loc(&a), loc(&b))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c1_class4_global_aggregate_over_per_partition_aggregate() {
    let (dir, a, b) = class4_env().await;
    let sql = format!(
        "SET nodra.broadcast_threshold = '1KB'; INSERT INTO delta('{}') \
         SELECT count(*) AS days, sum(s) AS total FROM \
         (SELECT a.day, sum(a.v) AS s FROM delta('{a}') a JOIN delta('{b}') b ON a.day = b.day AND a.cid = b.cid GROUP BY a.day) t",
        loc(&dir.path().join("c1"))
    );
    let o = outcome(&sql, 3).await;
    report("c1_class4_global_aggregate_over_per_partition_aggregate", &o);
    assert!(o.correct(), "{o:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c2_class4_top_n_over_per_partition_aggregate() {
    let (dir, a, b) = class4_env().await;
    let sql = format!(
        "SET nodra.broadcast_threshold = '1KB'; INSERT INTO delta('{}') \
         SELECT a.day, sum(a.v) AS s FROM delta('{a}') a JOIN delta('{b}') b ON a.day = b.day AND a.cid = b.cid \
         GROUP BY a.day ORDER BY s DESC LIMIT 2",
        loc(&dir.path().join("c2"))
    );
    let o = outcome(&sql, 3).await;
    report("c2_class4_top_n_over_per_partition_aggregate", &o);
    assert!(o.correct(), "{o:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn c3_class4_window_over_per_partition_aggregate() {
    let (dir, a, b) = class4_env().await;
    let sql = format!(
        "SET nodra.broadcast_threshold = '1KB'; INSERT INTO delta('{}') \
         SELECT day, s, rank() OVER (ORDER BY s DESC) AS r FROM \
         (SELECT a.day, sum(a.v) AS s FROM delta('{a}') a JOIN delta('{b}') b ON a.day = b.day AND a.cid = b.cid GROUP BY a.day) t",
        loc(&dir.path().join("c3"))
    );
    let o = outcome(&sql, 3).await;
    report("c3_class4_window_over_per_partition_aggregate", &o);
    assert!(o.correct(), "{o:?}");
}

// ------------------------------------------------------- D. incremental: driving choice and self-reads

/// The driving source is the largest table on the first run. When the dimension later outgrows
/// the fact table, the fact table must stay the driving source (its progress is in the sink),
/// or the whole join would be emitted again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d1_driving_source_flips_between_runs() {
    let env = env().await;
    let sql = env.sql("d1", "", "SELECT e.id, d.brand FROM {E} e JOIN {D} d ON e.cid = d.cid");
    let p = prepare(&sql).await.unwrap();
    println!("d1 run1 roles {:?}", p.plan.roles);
    let o1 = outcome(&sql, 2).await;
    report("d1 run1", &o1);
    // The dimension grows with keys that match nothing in events.
    append(std::path::Path::new(&env.d), dim_rows(1000..4000, 40)).await;
    let p = prepare(&sql).await.unwrap();
    println!("d1 run2 roles {:?}", p.plan.roles);
    run_once(&sql, 2).await.unwrap();
    let o2 = compare(&sql, p.plan.class.number()).await;
    report("d1_driving_source_flips_between_runs (after dim grew)", &o2);
    assert!(o2.correct(), "{o2:?}");
}

/// A second `delta()` call on the driving table would be a broadcast copy: right on the first
/// run, but an incremental run would join new rows with the full table and never revisit old
/// rows. Both shapes must be rejected.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn d2_second_read_of_the_driving_table_is_rejected() {
    let env = env().await;
    for (name, body) in [
        ("d2", "SELECT a.id AS a_id, b.id AS b_id FROM {E} a JOIN {E} b ON a.cid = b.cid AND a.day = b.day AND a.id < b.id"),
        ("d3", "SELECT id, v FROM {E} WHERE v > (SELECT avg(v) FROM {E})"),
    ] {
        let sql = env.sql(name, "", body);
        let err = prepare(&sql).await.unwrap_err();
        assert!(matches!(err, NodraError::Rejected(_)), "{name}: {err}");
    }
}

// --------------------------------------------------------- E. constructs that should be handled

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn e_handled_correctly() {
    let env = env().await;
    let cases: &[(&str, &str, &str)] = &[
        ("distinct", "", "SELECT DISTINCT cid, day FROM {E}"),
        ("distinct_on", "", "SELECT DISTINCT ON (cid) cid, id, v FROM {E} ORDER BY cid, v DESC"),
        ("rollup", "", "SELECT day, cid, count(*) AS n, sum(v) AS s FROM {E} GROUP BY ROLLUP (day, cid)"),
        ("cube", "", "SELECT day, cid, count(*) AS n FROM {E} GROUP BY CUBE (day, cid)"),
        ("grouping_sets_partitioned", ", partition_by => 'day'", "SELECT day, cid, count(*) AS n, grouping(cid) AS g FROM {E} GROUP BY GROUPING SETS ((day), (day, cid))"),
        ("ordered_aggs", "", "SELECT cid, array_agg(id ORDER BY id) AS ids, string_agg(user, ',' ORDER BY id) AS us, first_value(v ORDER BY id DESC) AS lastv, last_value(id ORDER BY v) AS lv FROM {E} GROUP BY cid"),
        ("multi_distinct", "", "SELECT day, count(DISTINCT user) AS u, count(DISTINCT cid) AS c, sum(DISTINCT cid) AS sc, array_agg(DISTINCT cid ORDER BY cid) AS cids FROM {E} GROUP BY day"),
        ("filter_aggs", "", "SELECT day, count(*) FILTER (WHERE v > 20) AS big, sum(v) FILTER (WHERE cid = 1) AS s1, count(DISTINCT user) FILTER (WHERE cid < 3) AS u FROM {E} GROUP BY day"),
        ("having", "", "SELECT cid, count(*) AS n FROM {E} GROUP BY cid HAVING count(*) > 17"),
        ("median_var", "", "SELECT day, median(v) AS m, stddev(v) AS sd, var_pop(v) AS vp, corr(v, id) AS c, bool_and(v > 1) AS ba FROM {E} GROUP BY day"),
        ("global_agg_sparse", "", "SELECT count(*) AS n, sum(v) AS s, min(id) AS lo FROM {E} WHERE id < 3"),
        ("global_agg_empty", "", "SELECT count(*) AS n, sum(v) AS s FROM {E} WHERE id < 0"),
        ("nested_agg", "", "SELECT n, count(*) AS k FROM (SELECT cid, day, count(*) AS n FROM {E} GROUP BY cid, day) t GROUP BY n"),
        ("window_above_agg", "", "SELECT cid, n, rank() OVER (ORDER BY n DESC, cid) AS r FROM (SELECT cid, count(*) AS n FROM {E} GROUP BY cid) t"),
        ("top_n_above_agg_full_merge", "", "SELECT cid, count(*) AS n FROM {E} GROUP BY cid ORDER BY n DESC, cid LIMIT 3"),
        ("count_distinct_partitioned", ", partition_by => 'day'", "SELECT day, count(DISTINCT user) AS u FROM {E} GROUP BY day"),
        ("semi_in", "", "SELECT id FROM {E} WHERE cid IN (SELECT cid FROM {D} WHERE brand = 'brand-1')"),
        ("exists_corr", "", "SELECT id FROM {E} e WHERE EXISTS (SELECT 1 FROM {D} d WHERE d.cid = e.cid AND d.brand <> 'brand-0')"),
        ("not_in", "", "SELECT id FROM {E} WHERE cid NOT IN (SELECT cid FROM {D})"),
        ("not_in_with_null", "", "SELECT id FROM {E} WHERE cid NOT IN (SELECT CASE WHEN cid = 5 THEN NULL ELSE cid END FROM {D})"),
        ("not_exists", "", "SELECT id FROM {E} e WHERE NOT EXISTS (SELECT 1 FROM {D} d WHERE d.cid = e.cid)"),
        ("scalar_subquery_broadcast", "", "SELECT id FROM {E} WHERE cid > (SELECT avg(cid) FROM {D})"),
        ("corr_scalar_subquery_broadcast", "", "SELECT e.id, (SELECT max(d.brand) FROM {D} d WHERE d.cid = e.cid) AS b FROM {E} e"),
        ("broadcast_preserved_via_agg_in", "", "SELECT d.cid, d.brand FROM {D} d WHERE d.cid IN (SELECT cid FROM {E} GROUP BY cid HAVING count(*) > 17)"),
        ("broadcast_preserved_via_corr_count", "", "SELECT d.cid, (SELECT count(*) FROM {E} e WHERE e.cid = d.cid AND e.v > 30) AS n FROM {D} d"),
        ("broadcast_left_join_agg", "", "SELECT d.cid, d.brand, t.n FROM {D} d LEFT JOIN (SELECT cid, count(*) AS n FROM {E} WHERE v > 55 GROUP BY cid) t ON d.cid = t.cid"),
        ("except_distinct", "", "SELECT cid FROM {E} EXCEPT SELECT cid FROM {D}"),
        ("intersect_distinct", "", "SELECT cid, day FROM {E} INTERSECT SELECT cid, 'x' FROM {D}"),
        ("left_join_filters", "", "SELECT e.id, d.brand FROM {E} e LEFT JOIN {D} d ON e.cid = d.cid AND d.brand <> 'brand-0' WHERE d.brand IS NULL OR e.v > 10"),
        ("right_join_driving_right", "", "SELECT e.id, d.brand FROM {D} d RIGHT JOIN {E} e ON e.cid = d.cid"),
        ("full_join_made_left", "", "SELECT e.id, d.brand FROM {E} e FULL JOIN {D} d ON e.cid = d.cid WHERE e.id IS NOT NULL"),
        ("left_join_broadcast_left_made_inner", "", "SELECT e.id, d.brand FROM {D} d LEFT JOIN {E} e ON e.cid = d.cid WHERE e.v > 3"),
        ("cross_join", "", "SELECT e.id, d.cid AS dc FROM {E} e CROSS JOIN {D} d WHERE e.id < 20"),
        ("values_join", "", "SELECT e.id, x.label FROM {E} e JOIN (VALUES (1, 'one'), (2, 'two')) AS x(cid, label) ON e.cid = x.cid"),
        ("broadcast_window_and_limit", "", "SELECT e.id, t.cid FROM {E} e JOIN (SELECT cid, row_number() OVER (ORDER BY cid) AS rn FROM {D} ORDER BY cid LIMIT 3) t ON e.cid = t.cid"),
        ("share_of_total_second_call", "", "SELECT cid, count(*) * 1.0 / (SELECT count(*) FROM {E}) AS share FROM {E} GROUP BY cid"),
        ("case_and_coalesce", "", "SELECT id, CASE WHEN v > 10 THEN 'hi' ELSE 'lo' END AS b, coalesce(user, 'x') AS u FROM {E}"),
    ];
    let mut bad = Vec::new();
    for (name, sink, body) in cases {
        let sql = env.sql(name, sink, body);
        let o = outcome(&sql, 3).await;
        report(name, &o);
        if !o.correct() {
            bad.push(format!("{name}: {o:?}"));
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
    // DataFusion 55 cannot execute IN subqueries in projections: the job fails at plan time.
    let sql = env.sql("in_projection", "", "SELECT id, cid IN (SELECT cid FROM {D} WHERE brand = 'brand-2') AS m FROM {E}");
    let err = prepare(&sql).await.unwrap_err();
    assert!(err.to_string().contains("cannot plan query"), "{err}");
}

// ----------------------------------------------- F. rejected, or correct with another driving source

/// Shapes where the largest table cannot be split. They are rejected, or the planner picks the
/// preserved side as the driving source and broadcasts the other one; either way the result
/// must be correct.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn f_rejected_or_correct() {
    let env = env().await;
    let cases: &[(&str, &str)] = &[
        ("limit", "SELECT id FROM {E} LIMIT 5"),
        ("offset", "SELECT id FROM {E} OFFSET 5"),
        ("limit_filter", "SELECT id FROM {E} WHERE v > 3 LIMIT 5"),
        ("limit_offset_sorted", "SELECT id FROM {E} ORDER BY id LIMIT 5 OFFSET 2"),
        ("window", "SELECT id, row_number() OVER (PARTITION BY cid ORDER BY id) AS rn FROM {E}"),
        ("window_in_subquery", "SELECT id FROM (SELECT id, row_number() OVER (PARTITION BY cid ORDER BY v DESC) AS rn FROM {E}) t WHERE rn = 1"),
        ("semi_driving_right", "SELECT d.cid FROM {D} d WHERE d.cid IN (SELECT cid FROM {E})"),
        ("anti_driving_right", "SELECT d.cid FROM {D} d WHERE NOT EXISTS (SELECT 1 FROM {E} e WHERE e.cid = d.cid)"),
        ("intersect_driving_right", "SELECT cid FROM {D} INTERSECT SELECT cid FROM {E}"),
        ("except_driving_right", "SELECT cid FROM {D} EXCEPT SELECT cid FROM {E}"),
        ("left_join_broadcast_preserved", "SELECT d.cid, e.id FROM {D} d LEFT JOIN {E} e ON e.cid = d.cid"),
        ("union_all_values", "SELECT id FROM {E} UNION ALL VALUES (1)"),
        ("cte_scalar_subquery", "WITH e AS (SELECT * FROM {E}) SELECT id FROM e WHERE v > (SELECT avg(v) FROM e)"),
        ("cte_share_of_total", "WITH e AS (SELECT * FROM {E}) SELECT cid, count(*) * 1.0 / (SELECT count(*) FROM e) AS share FROM e GROUP BY cid"),
        ("cte_semi_self", "WITH e AS (SELECT * FROM {E}) SELECT id FROM e WHERE cid IN (SELECT cid FROM e WHERE v > 50)"),
        ("cte_union_all", "WITH e AS (SELECT * FROM {E}) SELECT id FROM e WHERE v > 5 UNION ALL SELECT id FROM e WHERE cid = 1"),
        ("uncorrelated_scalar_over_driving_from_broadcast", "SELECT d.cid, (SELECT count(*) FROM {E}) AS total FROM {D} d"),
    ];
    let mut bad = Vec::new();
    for (name, body) in cases {
        let sql = env.sql(name, "", body);
        let o = outcome(&sql, 3).await;
        report(&format!("f/{name}"), &o);
        if !o.correct() {
            bad.push(format!("{name}: {o:?}"));
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
    // A preserved broadcast side that is not driving would emit its unmatched rows on every worker.
    let sql = env.sql("f_forced", "", "SELECT d.cid, e.id FROM delta('{DB}', broadcast => true) d LEFT JOIN {E} e ON e.cid = d.cid");
    let sql = sql.replace("{DB}", &env.d);
    let o = outcome(&sql, 3).await;
    assert!(matches!(o, Outcome::Rejected(_)), "{o:?}");
}

// ------------------------------------------------------------------ G. safe but (possibly) rejected

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn g_safe_but_rejected() {
    let env = env().await;
    // A dimension larger than the (small) fact table.
    let bigd = env.root.join("bigdim");
    create_table(&bigd, &dim_schema(), &[]).await;
    append(&bigd, dim_rows(0..6, 0)).await;
    append(&bigd, dim_rows(1000..4000, 40)).await;
    let left = format!(
        "INSERT INTO delta('{}') SELECT e.id, d.brand FROM delta('{}') e LEFT JOIN delta('{}') d ON e.cid = d.cid",
        env.out("g1"),
        env.e,
        loc(&bigd)
    );
    let o = outcome(&left, 3).await;
    report("g/left_join_dim_larger_than_fact", &o);
    assert!(matches!(o, Outcome::Matches { class: 2, .. }), "{o:?}");
    let hinted = left.replace(&format!("delta('{}')", loc(&bigd)), &format!("delta('{}', broadcast => true)", loc(&bigd))).replace("g1", "g1h");
    let o = outcome(&hinted, 3).await;
    report("g/left_join_dim_larger_than_fact_with_hint", &o);
    assert!(matches!(o, Outcome::Matches { class: 2, .. }), "{o:?}");

    let (dir, a, b) = class4_env().await;
    for (name, body) in [
        ("class4_group_by_right_partition_col", format!("SELECT b.day, count(*) AS n FROM delta('{a}') a JOIN delta('{b}') b ON a.day = b.day GROUP BY b.day")),
        ("class4_left_join", format!("SELECT a.id, b.w FROM delta('{a}') a LEFT JOIN delta('{b}') b ON a.day = b.day AND a.cid = b.cid")),
        ("class4_join_aggregated_large", format!("SELECT a.id, t.w FROM delta('{a}') a JOIN (SELECT day, sum(w) AS w FROM delta('{b}') GROUP BY day) t ON a.day = t.day")),
    ] {
        let sql = format!("SET nodra.broadcast_threshold = '1KB'; INSERT INTO delta('{}') {body}", loc(&dir.path().join(name)));
        let o = outcome(&sql, 3).await;
        report(&format!("g/{name}"), &o);
        assert!(o.correct(), "{name}: {o:?}");
        if name == "class4_group_by_right_partition_col" {
            assert!(matches!(o, Outcome::Matches { class: 4, .. }), "{o:?}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn h_ordered_aggregates_grouping_sets_and_subqueries() {
    let env = env().await;
    let cases: &[(&str, &str, &str)] = &[
        ("array_agg_ordered", "", "SELECT cid, array_agg(id ORDER BY id) AS ids FROM {E} GROUP BY cid"),
        ("string_agg_ordered", "", "SELECT cid, string_agg(user, ',' ORDER BY id) AS us FROM {E} GROUP BY cid"),
        ("first_value_desc", "", "SELECT cid, first_value(v ORDER BY id DESC) AS lastv FROM {E} GROUP BY cid"),
        ("last_value_asc", "", "SELECT cid, last_value(id ORDER BY v) AS lv FROM {E} GROUP BY cid"),
        ("first_and_last", "", "SELECT cid, first_value(v ORDER BY id DESC) AS a, last_value(id ORDER BY v) AS b FROM {E} GROUP BY cid"),
        ("rollup_with_join", "", "SELECT d.brand, e.day, count(*) AS n FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY ROLLUP (d.brand, e.day)"),
        ("grouping_sets_single_col", "", "SELECT day, cid, count(*) AS n FROM {E} GROUP BY GROUPING SETS ((day), (cid))"),
        ("subquery_first_in_text", "", "SELECT id, v - (SELECT avg(v) FROM {E}) AS dev FROM {E}"),
        ("corr_subquery_first_in_text", "", "SELECT o.id, (SELECT avg(i.v) FROM {E} i WHERE i.cid = o.cid) AS av FROM {E} o"),
    ];
    let mut bad = Vec::new();
    for (name, sink, body) in cases {
        for workers in [1, 3] {
            let sql = env.sql(&format!("{name}_{workers}"), sink, body);
            let o = outcome(&sql, workers).await;
            report(&format!("h/{name} ({workers} workers)"), &o);
            if !o.correct() {
                bad.push(format!("{name} ({workers} workers): {o:?}"));
            }
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}
