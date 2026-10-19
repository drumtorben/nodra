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
