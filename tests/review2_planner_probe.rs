//! Review probes (temporary): print worker / merge plans and check candidate bugs.
mod common;

use std::collections::BTreeSet;
use std::sync::Arc;

use common::*;
use datafusion::arrow::array::RecordBatch;
use datafusion::catalog::MemTable;
use datafusion::physical_plan::displayable;
use datafusion::physical_plan::empty::EmptyExec;
use nodra::exec::{self, Pipeline};
use nodra::job::{source_table, Job, SourceSpec};
use nodra::{delta, partial, session, NodraError};

/// Single-node reference that does not go through `FileListTable` filters: every Delta source
/// is read in full (no predicate) into a `MemTable`.
async fn mem_reference(sql: &str, streams: &[RecordBatch]) -> (Vec<String>, Vec<String>) {
    let job = Job::parse(sql, "ref").unwrap();
    let ctx = session::new_context(&job.settings).unwrap();
    for (i, spec) in job.sources.iter().enumerate() {
        match spec {
            SourceSpec::Delta { uri, .. } => {
                let t = delta::open(uri).await.unwrap();
                let files = delta::active_files(&t).unwrap();
                let tmp = session::new_context(&Default::default()).unwrap();
                exec::register_delta(&tmp, 0, &t, None, &files, false).unwrap();
                let df = tmp.sql(&format!("SELECT * FROM {}", source_table(0))).await.unwrap();
                let schema = Arc::new(df.schema().as_arrow().clone());
                let batches = df.collect().await.unwrap();
                ctx.register_table(source_table(i), Arc::new(MemTable::try_new(schema, vec![batches]).unwrap()))
                    .unwrap();
            }
            _ => {
                let b = streams[0].clone();
                ctx.register_table(source_table(i), Arc::new(MemTable::try_new(b.schema(), vec![vec![b]]).unwrap()))
                    .unwrap();
            }
        }
    }
    let df = ctx.sql(&job.query).await.unwrap();
    let names: Vec<String> = df.schema().fields().iter().map(|f| f.name().clone()).collect();
    let batches = df.collect().await.unwrap();
    (render(&batches), names)
}

#[derive(Debug)]
#[allow(dead_code)]
enum Outcome {
    Rejected(String),
    Failed(String),
    Matches { class: u8, rows: usize },
    Differs { class: u8, got: usize, expected: usize, missing: Vec<String>, unexpected: Vec<String> },
}

async fn compare(sql: &str, class: u8, streams: &[RecordBatch]) -> Outcome {
    let (expected, names) = mem_reference(sql, streams).await;
    let job = Job::parse(sql, "ref").unwrap();
    let got = table_rows(job.sink.uri(), &names).await;
    if got == expected {
        Outcome::Matches { class, rows: got.len() }
    } else {
        let e: BTreeSet<_> = expected.iter().cloned().collect();
        let g: BTreeSet<_> = got.iter().cloned().collect();
        Outcome::Differs {
            class,
            got: got.len(),
            expected: expected.len(),
            missing: e.difference(&g).take(4).cloned().collect(),
            unexpected: g.difference(&e).take(4).cloned().collect(),
        }
    }
}

async fn outcome(sql: &str, workers: u32, streams: Vec<RecordBatch>) -> Outcome {
    let sql = sql.to_string();
    tokio::spawn(async move {
        match Pipeline::prepare(Job::parse(&sql, "test").unwrap()).await {
            Err(NodraError::Rejected(r)) => return Outcome::Rejected(r.to_string()),
            Err(e) => return Outcome::Failed(format!("prepare: {e}")),
            Ok(_) => {}
        }
        let p = match run_once(&sql, workers).await {
            Ok(p) => p,
            Err(e) => return Outcome::Failed(format!("run: {e}")),
        };
        compare(&sql, p.plan.class.number(), &streams).await
    })
    .await
    .unwrap_or_else(|e| Outcome::Failed(format!("panic: {e}")))
}

/// Print the merge-time physical plan, its worker (partial) plan and the merge plan.
async fn print_plans(sql: &str) {
    let p = Pipeline::prepare(Job::parse(sql, "test").unwrap()).await.unwrap();
    println!("class {} scope {:?}", p.plan.class.number(), p.plan.merge_scope);
    let ctx = session::new_context(&p.job.settings).unwrap();
    exec::register_placeholders(&ctx, &p).await.unwrap();
    let physical = ctx.sql(&p.job.query).await.unwrap().create_physical_plan().await.unwrap();
    println!("--- placeholder plan\n{}", displayable(physical.as_ref()).indent(true));
    if p.plan.class.number() == 3 {
        let w = partial::worker_plan(&physical).unwrap();
        let states = partial::state_schema(&w.schema());
        let m = partial::merge_plan(&physical, Arc::new(EmptyExec::new(states))).unwrap();
        println!("--- merge plan\n{}", displayable(m.as_ref()).indent(true));
    }
    // Worker plan over the real files.
    let ctx = session::new_context(&p.job.settings).unwrap();
    for i in p.plan.driving() {
        if let SourceSpec::Delta { uri, .. } = &p.job.sources[i] {
            let t = delta::open(uri).await.unwrap();
            let files = delta::active_files(&t).unwrap();
            exec::register_delta(&ctx, i, &t, p.layouts[i].as_ref(), &files, true).unwrap();
        }
    }
    exec::register_broadcast(&ctx, &p, &p.plan.driving().into_iter().collect()).await.unwrap();
    if p.job.stream_source().is_none() {
        let physical = ctx.sql(&p.job.query).await.unwrap().create_physical_plan().await.unwrap();
        println!("--- worker full plan\n{}", displayable(physical.as_ref()).indent(true));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_scalar_subquery_below_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let (e, d) = (dir.path().join("e"), dir.path().join("d"));
    events_table(&e, 2, 30).await;
    dim_table(&d).await;
    let sql = format!(
        "INSERT INTO delta('{}') SELECT cid, count(*) AS n FROM delta('{}') WHERE cid > (SELECT avg(cid) FROM delta('{}')) GROUP BY cid",
        loc(&dir.path().join("o")),
        loc(&e),
        loc(&d)
    );
    print_plans(&sql).await;
    let o = outcome(&sql, 2, vec![]).await;
    println!("OUTCOME scalar_subquery_below_aggregate: {o:?}");
}

use datafusion::arrow::array::{ArrayRef, BooleanArray, Date32Array, Int32Array, Int64Array, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};

/// (cid, user, brand) for every cid 0..7 and user u0..u40 (matches `clicks`).
async fn cid_user_table(path: &std::path::Path) {
    cid_user_table_n(path, 41).await
}

async fn cid_user_table_n(path: &std::path::Path, nusers: i64) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("cid", DataType::Int64, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("label", DataType::Utf8, true),
    ]));
    create_table(path, &schema, &[]).await;
    let (mut c, mut u, mut l) = (Vec::new(), Vec::new(), Vec::new());
    for cid in 0..7i64 {
        for user in 0..nusers {
            c.push(cid);
            u.push(format!("u{user}"));
            l.push(format!("L{cid}-{user}"));
        }
    }
    let b = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int64Array::from(c)) as ArrayRef, Arc::new(StringArray::from(u)), Arc::new(StringArray::from(l))],
    )
    .unwrap();
    append(path, b).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_stream_join_above_aggregate_permuted_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, x) = (dir.path().join("clicks"), dir.path().join("x"));
    cid_user_table(&x).await;
    let mut all = Vec::new();
    for p in 0..3 {
        let (lines, batch) = clicks(p * 100..p * 100 + 40);
        append_jsonl(&stream, p as i32, &lines);
        all.push(batch);
    }
    for (name, body) in [
        ("perm", "SELECT t.user, t.cid, t.n, x.label FROM (SELECT user, cid, count(*) AS n FROM {S} GROUP BY user, cid) t JOIN {X} x ON t.cid = x.cid AND t.user = x.user"),
        ("perm_str", "SELECT t.user, t.c, t.n FROM (SELECT user, CAST(cid AS VARCHAR) AS c, count(*) AS n FROM {S} GROUP BY user, CAST(cid AS VARCHAR)) t JOIN {X} x ON t.c = CAST(x.cid AS VARCHAR) AND t.user = x.user"),
    ] {
        let sql = format!(
            "SET datafusion.execution.target_partitions = '4'; INSERT INTO delta('{}') {}",
            loc(&dir.path().join(name)),
            body.replace("{S}", &format!("jsonl_stream('{}', schema => 'id BIGINT, cid BIGINT, user VARCHAR')", loc(&stream)))
                .replace("{X}", &format!("delta('{}')", loc(&x)))
        );
        print_plans(&sql).await;
        // Worker-side plan over a real stream batch.
        let p = Pipeline::prepare(Job::parse(&sql, "test").unwrap()).await.unwrap();
        let ctx = session::new_context(&p.job.settings).unwrap();
        let b = concat(&all);
        let marked = partial::mark_driving(&p.infos[0].schema);
        let b = nodra::types::cast_batch(&b, &marked).unwrap();
        ctx.register_table(source_table(0), Arc::new(MemTable::try_new(marked, vec![vec![b]]).unwrap())).unwrap();
        exec::register_broadcast(&ctx, &p, &BTreeSet::from([0])).await.unwrap();
        let physical = ctx.sql(&p.job.query).await.unwrap().create_physical_plan().await.unwrap();
        println!("--- stream worker plan\n{}", displayable(physical.as_ref()).indent(true));
        let o = outcome(&sql, 2, vec![concat(&all)]).await;
        println!("OUTCOME stream {name}: {o:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_same_table_other_spelling() {
    let dir = tempfile::tempdir().unwrap();
    let e = dir.path().join("events");
    events_table(&e, 2, 30).await;
    let other = format!("{}/./events", loc(dir.path()));
    let sql = format!(
        "INSERT INTO delta('{}') SELECT a.id AS a_id, b.id AS b_id FROM delta('{}') a JOIN delta('{}') b ON a.cid = b.cid AND a.id < b.id",
        loc(&dir.path().join("o")),
        loc(&e),
        other
    );
    let o = outcome(&sql, 2, vec![]).await;
    println!("OUTCOME same_table run1: {o:?}");
    append(&e, events(60..90)).await;
    let p = run_once(&sql, 2).await;
    println!("run2: {:?}", p.as_ref().map(|p| p.plan.class.number()).map_err(|e| e.to_string()));
    let o = compare(&sql, 2, &[]).await;
    println!("OUTCOME same_table run2: {o:?}");
}

/// A table partitioned by DATE, INT, BOOLEAN, TIMESTAMP and STRING columns with NULLs and
/// hive-escaped values.
async fn typed_table(path: &std::path::Path) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Date32, true),
        Field::new("n", DataType::Int32, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new("s", DataType::Utf8, true),
    ]));
    create_table(path, &schema, &["d", "n", "b", "ts", "s"]).await;
    let strs = [Some("a/b c"), Some("100%=x"), Some("x=y"), None, Some("plain"), Some("%2F")];
    let rows = 36i64;
    let ids: Vec<i64> = (0..rows).collect();
    let d: Vec<Option<i32>> = ids.iter().map(|i| if i % 5 == 4 { None } else { Some(20_370 + (i % 3) as i32) }).collect();
    let n: Vec<Option<i32>> = ids.iter().map(|i| if i % 7 == 6 { None } else { Some((i % 4) as i32 - 1) }).collect();
    let b: Vec<Option<bool>> = ids.iter().map(|i| if i % 6 == 5 { None } else { Some(i % 2 == 0) }).collect();
    let ts: Vec<Option<i64>> =
        ids.iter().map(|i| if i % 8 == 7 { None } else { Some(1_760_000_000_000_000 + (i % 3) * 43_200_000_000) }).collect();
    let s: Vec<Option<&str>> = ids.iter().map(|i| strs[(*i as usize) % strs.len()]).collect();
    for chunk in 0..3 {
        let r = (chunk * 12) as usize..((chunk + 1) * 12) as usize;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids[r.clone()].to_vec())) as ArrayRef,
                Arc::new(Date32Array::from(d[r.clone()].to_vec())),
                Arc::new(Int32Array::from(n[r.clone()].to_vec())),
                Arc::new(BooleanArray::from(b[r.clone()].to_vec())),
                Arc::new(TimestampMicrosecondArray::from(ts[r.clone()].to_vec()).with_timezone("UTC")),
                Arc::new(StringArray::from(s[r].to_vec())),
            ],
        )
        .unwrap();
        append(path, batch).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_typed_partition_pruning() {
    let dir = tempfile::tempdir().unwrap();
    let (t, t2) = (dir.path().join("t"), dir.path().join("t2"));
    typed_table(&t).await;
    typed_table(&t2).await;
    let files = delta::active_files(&delta::open(&loc(&t)).await.unwrap()).unwrap();
    for f in files.iter().take(6) {
        println!("file {} {:?}", f.path, f.partition_values);
    }
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
        "n IN (SELECT n FROM {T2} WHERE id = 1)",
        "d = (SELECT max(d) FROM {T2})",
    ];
    let mut bad = Vec::new();
    for (k, f) in filters.iter().enumerate() {
        let f = f.replace("{T2}", &format!("delta('{}')", loc(&t2)));
        let sql = format!(
            "INSERT INTO delta('{}') SELECT id, d, n, b, ts, s FROM delta('{}') WHERE {f}",
            loc(&dir.path().join(format!("o{k}"))),
            loc(&t)
        );
        let o = outcome(&sql, 2, vec![]).await;
        println!("OUTCOME filter `{f}`: {o:?}");
        if !matches!(o, Outcome::Matches { .. }) {
            bad.push(format!("{f}: {o:?}"));
        }
    }
    // Join of two tables with the same partition column names.
    let sql = format!(
        "SET nodra.broadcast_threshold = '1GB'; INSERT INTO delta('{}') SELECT a.id, a.s AS sa, x.s AS sb FROM delta('{}') a JOIN delta('{}') x ON a.id = x.id + 1 \
         WHERE a.s = 'x=y' AND (x.s IS NULL OR x.n = 1)",
        loc(&dir.path().join("join")),
        loc(&t),
        loc(&t2)
    );
    let o = outcome(&sql, 2, vec![]).await;
    println!("OUTCOME join same partition names: {o:?}");
    // Class 3 with a partition filter and a partition-scoped merge.
    let sql = format!(
        "INSERT INTO delta('{}', partition_by => 's') SELECT s, n, count(*) AS k FROM delta('{}') WHERE n >= 0 OR s = 'a/b c' GROUP BY s, n",
        loc(&dir.path().join("agg")),
        loc(&t)
    );
    let o = outcome(&sql, 2, vec![]).await;
    println!("OUTCOME class3 typed: {o:?}");
    println!("BAD: {bad:#?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_delta_join_above_aggregate_permuted_keys() {
    let dir = tempfile::tempdir().unwrap();
    let (e, x) = (dir.path().join("e"), dir.path().join("x"));
    events_table(&e, 3, 40).await;
    cid_user_table_n(&x, 97).await;
    for (name, body) in [
        ("perm", "SELECT t.user, t.cid, t.n, x.label FROM (SELECT user, cid, count(*) AS n FROM {E} GROUP BY user, cid) t JOIN {X} x ON t.cid = x.cid AND t.user = x.user"),
        ("perm_str", "SELECT t.user, t.c, t.n FROM (SELECT user, CAST(cid AS VARCHAR) AS c, count(*) AS n FROM {E} GROUP BY user, CAST(cid AS VARCHAR)) t JOIN {X} x ON t.c = CAST(x.cid AS VARCHAR) AND t.user = x.user"),
        ("perm_day", "SELECT t.day, t.user, t.n FROM (SELECT day, user, count(*) AS n FROM {E} GROUP BY day, user) t JOIN (SELECT DISTINCT user, '2026-10-1' || CAST(cid % 3 AS VARCHAR) AS day FROM {X}) x ON t.user = x.user AND t.day = x.day"),
    ] {
        let sql = format!(
            "SET datafusion.execution.target_partitions = '4'; INSERT INTO delta('{}') {}",
            loc(&dir.path().join(name)),
            body.replace("{E}", &format!("delta('{}')", loc(&e))).replace("{X}", &format!("delta('{}')", loc(&x)))
        );
        print_plans(&sql).await;
        let o = outcome(&sql, 2, vec![]).await;
        println!("OUTCOME delta {name}: {o:?}");
    }
}
