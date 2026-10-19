//! Shapes verified correct by the second planner review: partition pruning, merge-plan
//! partitioning, scoped merges, by-name writes. Results are compared with a single-node
//! DataFusion run over `MemTable`s, so they do not depend on `FileListTable`'s pruning.
use crate::support::planner as support;

use support::*;

/// Partition pruning in `FileListTable::scan` on DATE, INT, BOOLEAN, TIMESTAMP and STRING
/// partition columns with NULLs and hive-escaped values (`a/b c`, `100%=x`, `x=y`, `%2F`), with
/// OR / IN / NOT / CAST / functions / CASE / subqueries / mixed partition-and-data filters.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn typed_partition_pruning_matches_single_node() {
    let dir = tempfile::tempdir().unwrap();
    let (t, t2) = (dir.path().join("t"), dir.path().join("t2"));
    typed_table(&t).await;
    typed_table(&t2).await;
    let filters = [
        "d = DATE '2025-10-08'",
        "d > DATE '2025-10-08'",
        "d IS NULL",
        "d IN (DATE '2025-10-08', DATE '2025-10-09')",
        "NOT (d = DATE '2025-10-08')",
        "CAST(d AS VARCHAR) LIKE '2025-10-0%'",
        "n = 1",
        "n > 0 OR n IS NULL",
        "n BETWEEN -1 AND 0",
        "n % 2 = 0",
        "abs(n) = 1",
        "-n = 1",
        "n NOT IN (0, 1)",
        "b",
        "NOT b",
        "b IS NOT TRUE",
        "b = false",
        "ts >= TIMESTAMP '2025-10-09T21:00:00Z'",
        "ts = TIMESTAMP '2025-10-09T08:53:20Z'",
        "date_trunc('day', ts) = TIMESTAMP '2025-10-09T00:00:00Z'",
        "ts IS NULL",
        "CAST(ts AS DATE) = DATE '2025-10-10'",
        "s = 'a/b c'",
        "s LIKE '100%'",
        "s = 'x=y'",
        "s = '%2F'",
        "s IS NULL",
        "length(s) > 4",
        "s IN ('a/b c', '100%=x')",
        "upper(s) = 'X=Y'",
        "s = 'x=y' OR id > 30",
        "coalesce(s, 'none') = 'none'",
        "CASE WHEN n IS NULL THEN 'null' ELSE 'x' END = 'null'",
        "s > 'b'",
        "s IS DISTINCT FROM 'plain'",
        "(n = 1) IS NOT TRUE",
        "concat(s, CAST(n AS VARCHAR)) = 'plain1'",
        "s = 'plain' AND n = 1 AND b",
        "n IN (SELECT n FROM {T2} WHERE id = 1)",
        "d = (SELECT max(d) FROM {T2})",
        "random() < 2.0 AND s = 'plain'",
    ];
    let mut bad = Vec::new();
    for (k, f) in filters.iter().enumerate() {
        let f = f.replace("{T2}", &format!("delta('{}')", loc(&t2)));
        let sql = format!(
            "INSERT INTO delta('{}') SELECT id, d, n, b, ts, s FROM delta('{}') WHERE {f}",
            loc(&dir.path().join(format!("o{k}"))),
            loc(&t)
        );
        let o = outcome(&sql, 3, vec![]).await;
        println!("OUTCOME filter `{f}`: {o:?}");
        if !matches!(o, Outcome::Matches { .. }) {
            bad.push(format!("{f}: {o:?}"));
        }
    }
    // Two tables with the same partition column names, filtered differently.
    let sql = format!(
        "INSERT INTO delta('{}') SELECT a.id, a.s AS sa, x.s AS sb, x.d AS db FROM delta('{}') a JOIN delta('{}') x ON a.id = x.id + 1 \
         WHERE a.s = 'x=y' AND x.s = '100%=x' AND x.d IS NOT NULL AND a.n IS DISTINCT FROM x.n",
        loc(&dir.path().join("join")),
        loc(&t),
        loc(&t2)
    );
    let o = outcome(&sql, 3, vec![]).await;
    println!("OUTCOME join with same partition names: {o:?}");
    if !matches!(o, Outcome::Matches { rows, .. } if rows > 0) {
        bad.push(format!("join: {o:?}"));
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

/// Class 3 with partition filters that leave some or all workers without files (their scan is an
/// `EmptyExec`, which must still carry the driving marker), and partition-scoped merges on
/// typed partition columns over incremental runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class3_pruned_workers_and_typed_scoped_merges() {
    let dir = tempfile::tempdir().unwrap();
    let t = dir.path().join("t");
    typed_table_chunks(&t, 0..2).await;
    let jobs: Vec<String> = [
        ("none", "", "SELECT n, count(*) AS k FROM {T} WHERE d = DATE '2025-10-08' GROUP BY n"),
        ("global_some", "", "SELECT count(*) AS k, sum(id) AS s, min(ts) AS first FROM {T} WHERE s = 'plain' AND n = 1"),
        ("global_none", "", "SELECT count(*) AS k, sum(id) AS s FROM {T} WHERE s = 'nope'"),
        ("by_date", ", partition_by => 'd'", "SELECT d, count(*) AS k, max(s) AS ms FROM {T} WHERE b IS NOT NULL GROUP BY d"),
        ("by_int_bool", ", partition_by => 'n, b'", "SELECT n, b, count(DISTINCT s) AS ds FROM {T} GROUP BY n, b"),
        ("by_ts", ", partition_by => 'ts'", "SELECT ts, sum(id) AS s FROM {T} WHERE s <> 'x=y' GROUP BY ts"),
        ("by_str", ", partition_by => 's'", "SELECT s, count(*) AS k FROM {T} GROUP BY s HAVING count(*) > 1"),
    ]
    .iter()
    .map(|(name, sink, body)| {
        format!("INSERT INTO delta('{}'{sink}) {}", loc(&dir.path().join(name)), body.replace("{T}", &format!("delta('{}')", loc(&t))))
    })
    .collect();
    let mut bad = Vec::new();
    for round in 0..2 {
        if round == 1 {
            typed_table_chunks(&t, 2..4).await;
        }
        for sql in &jobs {
            let o = outcome(sql, 3, vec![]).await;
            println!("OUTCOME round {round}: {o:?}  <- {sql}");
            if !matches!(o, Outcome::Matches { class: 3, .. }) {
                bad.push(format!("round {round} {sql}: {o:?}"));
            }
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

/// `merge_plan` replaces the split aggregate and must reproduce its output partitioning for the
/// operators above it: partitioned hash joins below the split on workers (SinglePartitioned),
/// joins / windows / top-N / grouping sets above it, scalar subqueries above it, mark joins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_plan_shapes_match_single_node() {
    let dir = tempfile::tempdir().unwrap();
    let (e, d, x) = (dir.path().join("e"), dir.path().join("d"), dir.path().join("x"));
    events_table(&e, 4, 30).await;
    dim_table(&d).await;
    cid_user_table(&x, 97).await;
    let cases: &[(&str, &str, &str)] = &[
        ("join_below_on_key", "", "SELECT e.cid, count(*) AS n FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY e.cid"),
        ("join_below_and_above", "", "SELECT t.cid, t.n, d2.brand FROM (SELECT e.cid, count(*) AS n FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY e.cid) t JOIN {D} d2 ON t.cid = d2.cid"),
        ("join_above_same_key_order", "", "SELECT t.user, t.cid, t.n, x.label FROM (SELECT user, cid, count(*) AS n FROM {E} GROUP BY user, cid) t JOIN {X} x ON t.user = x.user AND t.cid = x.cid"),
        ("join_above_subset_of_keys", "", "SELECT t.day, t.cid, t.n, d.brand FROM (SELECT day, cid, count(*) AS n FROM {E} GROUP BY day, cid) t JOIN {D} d ON t.cid = d.cid"),
        ("left_join_above", "", "SELECT d.cid, d.brand, t.n FROM {D} d LEFT JOIN (SELECT cid, count(*) AS n FROM {E} WHERE v > 20 GROUP BY cid) t ON d.cid = t.cid"),
        ("rollup_join_below", "", "SELECT d.brand, e.day, count(*) AS n, grouping(e.day) AS g FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY ROLLUP (d.brand, e.day)"),
        ("cube_join_below", "", "SELECT d.brand, e.cid, sum(e.v) AS s FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY CUBE (d.brand, e.cid)"),
        ("window_partitioned_by_key", "", "SELECT cid, day, n, rank() OVER (PARTITION BY cid ORDER BY n DESC, day) AS r FROM (SELECT cid, day, count(*) AS n FROM {E} GROUP BY cid, day) t"),
        ("window_over_join_below", "", "SELECT cid, n, sum(n) OVER (PARTITION BY cid) AS tot FROM (SELECT e.cid, count(*) AS n FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY e.cid) t"),
        ("topk_max", "", "SELECT cid, max(v) AS m FROM {E} GROUP BY cid ORDER BY m DESC LIMIT 3"),
        ("topk_min_join_below", "", "SELECT d.brand, min(e.v) AS m FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY d.brand ORDER BY m LIMIT 2"),
        ("having_scalar_subquery", "", "SELECT cid, count(*) AS n FROM {E} GROUP BY cid HAVING count(*) > (SELECT count(*) * 2 FROM {D})"),
        ("projection_scalar_subquery", "", "SELECT cid, count(*) * (SELECT count(*) FROM {D}) AS x FROM {E} GROUP BY cid"),
        ("mark_join", "", "SELECT id FROM {E} WHERE cid IN (SELECT cid FROM {D} WHERE brand = 'brand-1') OR v > 50"),
        ("distinct_join_below", ", partition_by => 'brand'", "SELECT DISTINCT d.brand, e.day FROM {E} e JOIN {D} d ON e.cid = d.cid"),
        ("ordered_aggs_join_below", "", "SELECT e.cid, array_agg(e.id ORDER BY e.id) AS ids, first_value(e.v ORDER BY e.id DESC) AS lastv FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY e.cid"),
        ("union_above_split", "", "SELECT cid, count(*) AS n FROM {E} GROUP BY cid UNION ALL SELECT cid, 0 AS n FROM {D}"),
        ("nested_agg_join_below", "", "SELECT n, count(*) AS k FROM (SELECT e.cid, e.day, count(*) AS n FROM {E} e JOIN {D} d ON e.cid = d.cid GROUP BY e.cid, e.day) t GROUP BY n"),
    ];
    let mut bad = Vec::new();
    for (name, sink, body) in cases {
        let sql = format!(
            "SET datafusion.execution.target_partitions = '4'; INSERT INTO delta('{}'{sink}) {}",
            loc(&dir.path().join(name)),
            body.replace("{E}", &format!("delta('{}')", loc(&e)))
                .replace("{D}", &format!("delta('{}')", loc(&d)))
                .replace("{X}", &format!("delta('{}')", loc(&x)))
        );
        let o = outcome(&sql, 3, vec![]).await;
        println!("OUTCOME {name}: {o:?}");
        if !o.correct() {
            bad.push(format!("{name}: {o:?}"));
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}

/// `Columns::ByName` / `cast_batch_by_name`: the query's column order changes between runs of the
/// same job (the sink was created by the earlier version), for class 1 and for the class-3 merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn by_name_writes_after_column_reorder() {
    let dir = tempfile::tempdir().unwrap();
    let e = dir.path().join("e");
    events_table(&e, 3, 40).await;
    let mut bad = Vec::new();
    for (name, v1, v2) in [
        ("class1", "SELECT id, cid, day FROM {E}", "SELECT day, cid, id FROM {E}"),
        (
            "class3",
            "SELECT day, cid, count(*) AS n, sum(v) AS s FROM {E} GROUP BY day, cid",
            "SELECT s, n, cid, day FROM (SELECT day, cid, count(*) AS n, sum(v) AS s FROM {E} GROUP BY day, cid) t",
        ),
    ] {
        for (k, body) in [v1, v2].iter().enumerate() {
            let sql = format!(
                "INSERT INTO delta('{}', partition_by => 'day') {}",
                loc(&dir.path().join(name)),
                body.replace("{E}", &format!("delta('{}')", loc(&e)))
            );
            if k == 1 {
                append(&e, events(500 + 100 * (name.len() as i64)..530 + 100 * (name.len() as i64))).await;
            }
            let o = outcome(&sql, 2, vec![]).await;
            println!("OUTCOME {name} v{}: {o:?}", k + 1);
            if !matches!(o, Outcome::Matches { .. }) {
                bad.push(format!("{name} v{}: {o:?}", k + 1));
            }
        }
    }
    assert!(bad.is_empty(), "{bad:#?}");
}
