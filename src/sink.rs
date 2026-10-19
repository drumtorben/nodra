//! Writing query results as Parquet files into a Delta table directory.
//!
//! The files are only *written* here; they become part of the table when the returned `add`
//! actions are committed together with the batch's progress (see `exec`).

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::SendableRecordBatchStream;
use deltalake_core::kernel::Add;
use deltalake_core::writer::{DeltaWriter, RecordBatchWriter};
use deltalake_core::DeltaTable;
use futures::StreamExt;

use crate::error::Result;
use crate::types::{cast_batch, cast_batch_by_name};

/// Result of writing a stream.
#[derive(Debug, Default)]
pub struct Written {
    /// `add` actions for the new files.
    pub adds: Vec<Add>,
    /// Rows written.
    pub rows: u64,
}

/// How result columns map to the table's columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Columns {
    /// By name (query results written to a sink).
    ByName,
    /// By position (partial states, stored under positional names `s0..sN`).
    ByPosition,
}

/// Write `stream` into `table`'s directory. Every batch is cast to `target` (the table's Arrow
/// schema), matching columns as `columns` says.
pub async fn write_stream(
    table: &DeltaTable,
    target: &SchemaRef,
    columns: Columns,
    mut stream: SendableRecordBatchStream,
) -> Result<Written> {
    let mut writer = RecordBatchWriter::for_table(table)?;
    let mut rows = 0u64;
    while let Some(batch) = stream.next().await {
        let batch = batch?;
        if batch.num_rows() == 0 {
            continue;
        }
        let batch = match columns {
            Columns::ByName => cast_batch_by_name(&batch, target)?,
            Columns::ByPosition => cast_batch(&batch, target)?,
        };
        rows += batch.num_rows() as u64;
        writer.write(batch).await?;
    }
    let adds = if rows == 0 { Vec::new() } else { writer.flush().await? };
    Ok(Written { adds, rows })
}
