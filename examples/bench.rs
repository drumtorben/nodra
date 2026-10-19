//! A small throughput benchmark: generate an events table, then run an enrichment job
//! (class 2) and a daily aggregation (class 3) with local workers.
//!
//! ```sh
//! cargo run --release --example bench -- /tmp/nodra-bench 20000000 40 2
//! #                                       dir             rows     files workers
//! ```

use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake_core::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake_core::kernel::StructType;
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use nodra::exec::{self, Pipeline};
use nodra::job::Job;
use tokio::sync::watch;

async fn create(path: &Path, schema: &SchemaRef, partitions: &[&str]) -> nodra::Result<()> {
    let kernel: StructType = schema.as_ref().try_into_kernel()?;
    let url = nodra::delta::table_url(&path.to_string_lossy())?;
    let table = deltalake_core::DeltaTableBuilder::from_url(url)?.build()?;
    CreateBuilder::new()
        .with_log_store(table.log_store())
        .with_columns(kernel.fields().cloned())
        .with_partition_columns(partitions.iter().map(|s| s.to_string()))
        .await?;
    Ok(())
}

fn events(schema: &SchemaRef, from: i64, to: i64) -> nodra::Result<RecordBatch> {
    let start_us: i64 = 1_792_195_200_000_000; // 2026-10-17T00:00:00Z
    let ids: Vec<i64> = (from..to).collect();
    let ts: Vec<i64> = ids.iter().map(|i| start_us + (i % 345_600) * 1_000_000).collect();
    let day: Vec<String> = ts
        .iter()
        .map(|t| chrono::DateTime::from_timestamp_micros(*t).unwrap().format("%Y-%m-%d").to_string())
        .collect();
    Ok(RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
            Arc::new(Int64Array::from(ids.iter().map(|i| (i * 7919) % 1000).collect::<Vec<_>>())),
            Arc::new(StringArray::from(ids.iter().map(|i| format!("user-{}", (i * 104_729) % 1_000_000)).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(ids.iter().map(|i| (i % 97) as f64 * 0.25).collect::<Vec<_>>())),
            Arc::new(StringArray::from(day)),
        ],
    )?)
}

async fn run(sql: &str, workers: u32) -> nodra::Result<f64> {
    let p = Arc::new(Pipeline::prepare(Job::parse(sql, "bench")?).await?);
    let (_tx, rx) = watch::channel(false);
    let started = Instant::now();
    exec::run(p, (0..workers).collect(), workers, true, rx).await?;
    Ok(started.elapsed().as_secs_f64())
}

#[tokio::main]
async fn main() -> nodra::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let dir = std::path::PathBuf::from(args.get(1).cloned().unwrap_or_else(|| "/tmp/nodra-bench".into()));
    let rows: i64 = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5_000_000);
    let files: i64 = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(20);
    let workers: u32 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(2);
    let (src, dim) = (dir.join("events"), dir.join("campaigns"));

    if !src.exists() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("event_id", DataType::Int64, true),
            Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
            Field::new("campaign_id", DataType::Int64, true),
            Field::new("user_id", DataType::Utf8, true),
            Field::new("value", DataType::Float64, true),
            Field::new("day", DataType::Utf8, true),
        ]));
        create(&src, &schema, &["day"]).await?;
        let per = rows / files;
        let started = Instant::now();
        for f in 0..files {
            let mut table = nodra::delta::open(&src.to_string_lossy()).await?;
            let mut w = RecordBatchWriter::for_table(&table)?;
            let mut at = f * per;
            while at < (f + 1) * per {
                let to = (at + 500_000).min((f + 1) * per);
                w.write(events(&schema, at, to)?).await?;
                at = to;
            }
            w.flush_and_commit(&mut table).await?;
        }
        let c_schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("campaign_id", DataType::Int64, true),
            Field::new("brand", DataType::Utf8, true),
        ]));
        create(&dim, &c_schema, &[]).await?;
        let ids: Vec<i64> = (0..1000).collect();
        let mut table = nodra::delta::open(&dim.to_string_lossy()).await?;
        let mut w = RecordBatchWriter::for_table(&table)?;
        w.write(RecordBatch::try_new(
            c_schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
                Arc::new(StringArray::from(ids.iter().map(|i| format!("brand-{}", i % 40)).collect::<Vec<_>>())),
            ],
        )?)
        .await?;
        w.flush_and_commit(&mut table).await?;
        println!("generated {rows} rows in {files} commits in {:.1}s", started.elapsed().as_secs_f64());
    }

    for out in ["silver", "gold", "gold__nodra_partials"] {
        let _ = std::fs::remove_dir_all(dir.join(out));
    }
    let enrich = format!(
        "SET nodra.job_name = 'bench_enrich'; INSERT INTO delta('{}', partition_by => 'day') \
         SELECT e.event_id, e.ts, e.user_id, e.value, c.brand, e.day \
         FROM delta('{}') e JOIN delta('{}') c ON e.campaign_id = c.campaign_id",
        dir.join("silver").display(),
        src.display(),
        dim.display()
    );
    let daily = format!(
        "SET nodra.job_name = 'bench_daily'; INSERT INTO delta('{}', partition_by => 'day') \
         SELECT e.day, c.brand, count(*) AS events, sum(e.value) AS value, approx_distinct(e.user_id) AS users \
         FROM delta('{}') e JOIN delta('{}') c ON e.campaign_id = c.campaign_id GROUP BY e.day, c.brand",
        dir.join("gold").display(),
        src.display(),
        dim.display()
    );
    for (name, sql) in [("class 2 enrichment", &enrich), ("class 3 daily aggregation", &daily)] {
        let secs = run(sql, workers).await?;
        println!(
            "{name}: {rows} rows with {workers} local workers in {secs:.1}s ({:.2} M rows/s)",
            rows as f64 / secs / 1e6
        );
    }
    Ok(())
}
