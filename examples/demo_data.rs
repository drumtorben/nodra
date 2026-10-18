//! Create small demo tables for the quickstart:
//!
//! ```sh
//! cargo run --example demo_data -- /tmp/nodra-demo
//! ```
//!
//! * `<dir>/bronze/events`   – Delta, partitioned by `day`, 5 commits of 2 000 events
//! * `<dir>/dim/campaigns`   – Delta, 50 campaigns
//! * `<dir>/stream/clicks`   – `jsonl_stream` directory with 4 partitions

use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit};
use deltalake_core::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake_core::kernel::StructType;
use deltalake_core::operations::create::CreateBuilder;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};

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

async fn append(path: &Path, batch: RecordBatch) -> nodra::Result<()> {
    let mut table = nodra::delta::open(&path.to_string_lossy()).await?;
    let mut w = RecordBatchWriter::for_table(&table)?;
    w.write(batch).await?;
    w.flush_and_commit(&mut table).await?;
    Ok(())
}

#[tokio::main]
async fn main() -> nodra::Result<()> {
    let dir = PathBuf::from(std::env::args().nth(1).unwrap_or_else(|| "/tmp/nodra-demo".into()));
    let events = dir.join("bronze/events");
    let campaigns = dir.join("dim/campaigns");
    let stream = dir.join("stream/clicks");

    let ev_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("event_id", DataType::Int64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new("campaign_id", DataType::Int64, true),
        Field::new("user_id", DataType::Utf8, true),
        Field::new("event_type", DataType::Utf8, true),
        Field::new("day", DataType::Utf8, true),
    ]));
    create(&events, &ev_schema, &["day"]).await?;
    let start_us: i64 = 1_792_195_200_000_000; // 2026-10-17T00:00:00Z
    for commit in 0..5i64 {
        let ids: Vec<i64> = (commit * 2000..(commit + 1) * 2000).collect();
        let ts: Vec<i64> = ids.iter().map(|i| start_us + i * 37_000_000).collect();
        let day: Vec<String> = ts
            .iter()
            .map(|t| chrono::DateTime::from_timestamp_micros(*t).unwrap().format("%Y-%m-%d").to_string())
            .collect();
        let batch = RecordBatch::try_new(
            ev_schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                Arc::new(Int64Array::from(ids.iter().map(|i| (i * 7919) % 50).collect::<Vec<_>>())),
                Arc::new(StringArray::from(ids.iter().map(|i| format!("user-{}", (i * 104_729) % 3000)).collect::<Vec<_>>())),
                Arc::new(StringArray::from(
                    ids.iter().map(|i| if i % 5 == 0 { "click" } else { "impression" }).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(day)),
            ],
        )?;
        append(&events, batch).await?;
    }

    let c_schema: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("campaign_id", DataType::Int64, true),
        Field::new("brand", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("cpm", DataType::Float64, true),
    ]));
    create(&campaigns, &c_schema, &[]).await?;
    let ids: Vec<i64> = (0..50).collect();
    let brands = ["Aurora", "Borealis", "Cirrus", "Dorado", "Eos"];
    let countries = ["DE", "FR", "IT", "ES", "NL", "PL"];
    append(
        &campaigns,
        RecordBatch::try_new(
            c_schema,
            vec![
                Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
                Arc::new(StringArray::from(ids.iter().map(|i| brands[*i as usize % 5]).collect::<Vec<_>>())),
                Arc::new(StringArray::from(ids.iter().map(|i| countries[*i as usize % 6]).collect::<Vec<_>>())),
                Arc::new(Float64Array::from(ids.iter().map(|i| 1.5 + (*i as f64) * 0.1).collect::<Vec<_>>())),
            ],
        )?,
    )
    .await?;

    std::fs::create_dir_all(&stream)?;
    for p in 0..4i64 {
        let mut f = std::fs::File::create(stream.join(format!("{p}.jsonl")))?;
        for i in 0..500i64 {
            let id = p * 1_000_000 + i;
            writeln!(
                f,
                r#"{{"ts":"2026-10-18T{:02}:{:02}:00Z","campaign_id":{},"user_id":"user-{}"}}"#,
                (i / 60) % 24,
                i % 60,
                (id * 31) % 50,
                (id * 17) % 700
            )?;
        }
    }
    println!("demo data written to {}", dir.display());
    Ok(())
}
