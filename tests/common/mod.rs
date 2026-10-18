//! Test helpers: build Delta tables, run jobs in-process, compare results with a single-node
//! reference computed by DataFusion over the complete inputs.
#![allow(dead_code)]

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;

use datafusion::arrow::array::{Array, ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::catalog::MemTable;
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use deltalake_core::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake_core::kernel::StructType;
use nodra::exec::{self, Pipeline};
use nodra::job::{source_table, Job, SourceSpec};
use nodra::{delta, session};
use tokio::sync::watch;

pub fn loc(p: &Path) -> String {
    p.to_string_lossy().to_string()
}

/// Create an empty Delta table.
pub async fn create_table(path: &Path, schema: &SchemaRef, partitions: &[&str]) {
    let kernel: StructType = schema.as_ref().try_into_kernel().unwrap();
    let url = delta::table_url(&loc(path)).unwrap();
    let table = deltalake_core::DeltaTableBuilder::from_url(url).unwrap().build().unwrap();
    CreateBuilder::new()
        .with_log_store(table.log_store())
        .with_columns(kernel.fields().cloned())
        .with_partition_columns(partitions.iter().map(|s| s.to_string()))
        .await
        .unwrap();
}

/// Append a batch as one commit; returns the new version.
pub async fn append(path: &Path, batch: RecordBatch) -> u64 {
    let mut table = delta::open(&loc(path)).await.unwrap();
    let mut w = RecordBatchWriter::for_table(&table).unwrap();
    w.write(batch).await.unwrap();
    w.flush_and_commit(&mut table).await.unwrap()
}

pub fn events_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("cid", DataType::Int64, true),
        Field::new("user", DataType::Utf8, true),
        Field::new("v", DataType::Float64, true),
        Field::new("day", DataType::Utf8, true),
    ]))
}

/// Deterministic synthetic events with ids in `range`.
pub fn events(range: std::ops::Range<i64>) -> RecordBatch {
    let ids: Vec<i64> = range.collect();
    let cid: Vec<i64> = ids.iter().map(|i| i % 7).collect();
    let user: Vec<String> = ids.iter().map(|i| format!("u{}", (i * 31) % 97)).collect();
    let v: Vec<f64> = ids.iter().map(|i| (*i as f64) * 0.5).collect();
    let day: Vec<String> = ids.iter().map(|i| format!("2026-10-{:02}", 10 + (i % 3))).collect();
    RecordBatch::try_new(
        events_schema(),
        vec![
            Arc::new(Int64Array::from(ids)) as ArrayRef,
            Arc::new(Int64Array::from(cid)),
            Arc::new(StringArray::from(user)),
            Arc::new(Float64Array::from(v)),
            Arc::new(StringArray::from(day)),
        ],
    )
    .unwrap()
}

pub fn dim_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("cid", DataType::Int64, true),
        Field::new("brand", DataType::Utf8, true),
    ]))
}

pub fn dim() -> RecordBatch {
    let cid: Vec<i64> = (0..6).collect(); // cid 6 has no brand: tests inner vs left joins
    let brand: Vec<String> = cid.iter().map(|c| format!("brand-{}", c % 3)).collect();
    RecordBatch::try_new(dim_schema(), vec![Arc::new(Int64Array::from(cid)), Arc::new(StringArray::from(brand))]).unwrap()
}

/// Events table with `commits` appends of `per` rows each, partitioned by day.
pub async fn events_table(path: &Path, commits: i64, per: i64) {
    create_table(path, &events_schema(), &["day"]).await;
    for c in 0..commits {
        append(path, events(c * per..(c + 1) * per)).await;
    }
}

pub async fn dim_table(path: &Path) {
    create_table(path, &dim_schema(), &[]).await;
    append(path, dim()).await;
}

/// Parse a job and run it with `workers` local workers until caught up.
pub async fn run_once(sql: &str, workers: u32) -> nodra::Result<Arc<Pipeline>> {
    let job = Job::parse(sql, "test")?;
    let p = Arc::new(Pipeline::prepare(job).await?);
    let (_tx, rx) = watch::channel(false);
    exec::run(p.clone(), (0..workers).collect(), workers, true, rx).await?;
    Ok(p)
}

/// Rows of a Delta table, columns ordered like `columns`, rendered and sorted.
pub async fn table_rows(location: &str, columns: &[String]) -> Vec<String> {
    let t = delta::open(location).await.unwrap();
    let ctx = session::new_context(&Default::default()).unwrap();
    let files = delta::active_files(&t).unwrap();
    exec::register_delta(&ctx, 0, &t, &files, false).unwrap();
    let cols = columns.iter().map(|c| format!("\"{c}\"")).collect::<Vec<_>>().join(", ");
    let batches = ctx.sql(&format!("SELECT {cols} FROM {}", source_table(0))).await.unwrap().collect().await.unwrap();
    render(&batches)
}

/// Single-node reference: the job's query over the complete inputs, in one DataFusion context.
/// `streams` supplies the full content of stream sources.
pub async fn reference(sql: &str, streams: &[RecordBatch]) -> (Vec<String>, Vec<String>) {
    let job = Job::parse(sql, "ref").unwrap();
    let ctx = session::new_context(&job.settings).unwrap();
    for (i, spec) in job.sources.iter().enumerate() {
        match spec {
            SourceSpec::Delta { uri, .. } => {
                let t = delta::open(uri).await.unwrap();
                let files = delta::active_files(&t).unwrap();
                exec::register_delta(&ctx, i, &t, &files, false).unwrap();
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

/// Render rows as strings (floats rounded to 6 decimals), sorted.
pub fn render(batches: &[RecordBatch]) -> Vec<String> {
    let mut rows = Vec::new();
    let opts = FormatOptions::default().with_null("NULL");
    for b in batches {
        let fmts: Vec<_> = b
            .columns()
            .iter()
            .map(|c| (c.clone(), ArrayFormatter::try_new(c.as_ref(), &opts).unwrap()))
            .collect();
        for r in 0..b.num_rows() {
            let cells: Vec<String> = fmts
                .iter()
                .map(|(arr, f)| {
                    if arr.is_null(r) {
                        return "NULL".to_string();
                    }
                    match arr.data_type() {
                        DataType::Float64 | DataType::Float32 => {
                            let v: f64 = f.value(r).to_string().parse().unwrap();
                            format!("{v:.6}")
                        }
                        _ => f.value(r).to_string(),
                    }
                })
                .collect();
            rows.push(cells.join(" | "));
        }
    }
    rows.sort();
    rows
}

/// Assert the sink equals the single-node reference.
pub async fn assert_matches_reference(sql: &str, streams: &[RecordBatch]) -> usize {
    let (expected, names) = reference(sql, streams).await;
    let job = Job::parse(sql, "ref").unwrap();
    let got = table_rows(job.sink.uri(), &names).await;
    if got != expected {
        let e: BTreeSet<_> = expected.iter().collect();
        let g: BTreeSet<_> = got.iter().collect();
        panic!(
            "sink differs from reference\n  rows: got {}, expected {}\n  missing: {:?}\n  unexpected: {:?}",
            got.len(),
            expected.len(),
            e.difference(&g).take(5).collect::<Vec<_>>(),
            g.difference(&e).take(5).collect::<Vec<_>>()
        );
    }
    expected.len()
}
