//! Helpers shared by the `planner_regressions` and `planner_shapes` tests.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BooleanArray, Date32Array, Int32Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::catalog::MemTable;
use nodra::exec::{self, Pipeline};
use nodra::job::{source_table, Job, SourceSpec};
use nodra::{delta, session, NodraError};

pub use crate::common::*;

/// Single-node reference that does not depend on `FileListTable` filter handling: every Delta
/// source is read in full (no predicate) into a `MemTable`, then the job's query runs on those.
pub async fn mem_reference(sql: &str, streams: &[RecordBatch]) -> (Vec<String>, Vec<String>) {
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
pub enum Outcome {
    Rejected(String),
    Failed(String),
    Matches { class: u8, rows: usize },
    Differs { class: u8, got: usize, expected: usize, missing: Vec<String>, unexpected: Vec<String> },
}

impl Outcome {
    pub fn correct(&self) -> bool {
        matches!(self, Outcome::Rejected(_) | Outcome::Matches { .. })
    }
}

/// Compare the sink with the reference (a missing sink counts as empty).
pub async fn compare(sql: &str, class: u8, streams: &[RecordBatch]) -> Outcome {
    let (expected, names) = mem_reference(sql, streams).await;
    let job = Job::parse(sql, "ref").unwrap();
    let got = match delta::try_open(job.sink.uri()).await.unwrap() {
        Some(_) => table_rows(job.sink.uri(), &names).await,
        None => Vec::new(),
    };
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

/// Plan, run with `workers` workers until caught up, compare with the reference.
pub async fn outcome(sql: &str, workers: u32, streams: Vec<RecordBatch>) -> Outcome {
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

/// `(cid, user, label)` for every cid 0..7 and user u0..u{nusers-1}.
pub async fn cid_user_table(path: &Path, nusers: i64) {
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

/// `(cid, w, day)` rows: `per_day` rows for each day.
pub fn spend(days: &[String], per_day: i64) -> RecordBatch {
    let (mut day, mut cid, mut w) = (Vec::new(), Vec::new(), Vec::new());
    for d in days {
        for c in 0..per_day {
            day.push(d.clone());
            cid.push(c % 7);
            w.push(c as f64 * 1.5);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("cid", DataType::Int64, true),
            Field::new("w", DataType::Float64, true),
            Field::new("day", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from(cid)) as ArrayRef,
            Arc::new(datafusion::arrow::array::Float64Array::from(w)),
            Arc::new(StringArray::from(day)),
        ],
    )
    .unwrap()
}

pub async fn table_size(path: &Path) -> u64 {
    delta::active_files(&delta::open(&loc(path)).await.unwrap()).unwrap().iter().map(|f| f.size).sum()
}

/// A table partitioned by DATE, INT, BOOLEAN, TIMESTAMP and STRING columns, with NULL and
/// hive-escaped partition values, in 3 commits (36 rows, many files).
pub async fn typed_table(path: &Path) {
    typed_table_chunks(path, 0..3).await
}

/// Like [`typed_table`], appending only commits `chunks` (the table is created for chunk 0).
pub async fn typed_table_chunks(path: &Path, chunks: std::ops::Range<usize>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("d", DataType::Date32, true),
        Field::new("n", DataType::Int32, true),
        Field::new("b", DataType::Boolean, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new("s", DataType::Utf8, true),
    ]));
    if chunks.start == 0 {
        create_table(path, &schema, &["d", "n", "b", "ts", "s"]).await;
    }
    let strs = [Some("a/b c"), Some("100%=x"), Some("x=y"), None, Some("plain"), Some("%2F")];
    let ids: Vec<i64> = (0..48).collect();
    let d: Vec<Option<i32>> = ids.iter().map(|i| if i % 5 == 4 { None } else { Some(20_370 + (i % 3) as i32) }).collect();
    let n: Vec<Option<i32>> = ids.iter().map(|i| if i % 7 == 6 { None } else { Some((i % 4) as i32 - 1) }).collect();
    let b: Vec<Option<bool>> = ids.iter().map(|i| if i % 6 == 5 { None } else { Some(i % 2 == 0) }).collect();
    let ts: Vec<Option<i64>> =
        ids.iter().map(|i| if i % 8 == 7 { None } else { Some(1_760_000_000_000_000 + (i % 3) * 43_200_000_000) }).collect();
    let s: Vec<Option<&str>> = ids.iter().map(|i| strs[(*i as usize) % strs.len()]).collect();
    for chunk in chunks {
        let r = chunk * 12..(chunk + 1) * 12;
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
