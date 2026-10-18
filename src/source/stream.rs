//! Stream sources: partitioned, offset-addressed logs (Kafka, `jsonl_stream`).

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::json::ReaderBuilder;

use crate::error::{NodraError, Result};

/// Messages read from one partition.
#[derive(Debug, Default)]
pub struct Fetched {
    /// Raw message payloads in offset order (`None` for tombstones).
    pub payloads: Vec<Option<Vec<u8>>>,
    /// Offset of the first payload.
    pub first_offset: i64,
    /// Offset to continue from (one past the last message read).
    pub next_offset: i64,
}

/// A partitioned log.
#[async_trait]
pub trait StreamSource: Send + Sync + std::fmt::Debug {
    /// Partition ids.
    async fn partitions(&self) -> Result<Vec<i32>>;
    /// `(low, high)`: first available offset and the offset after the last message.
    async fn watermarks(&self, partition: i32) -> Result<(i64, i64)>;
    /// Read up to `max` messages starting at `from`, stopping at `until` (exclusive).
    async fn fetch(&self, partition: i32, from: i64, until: i64, max: usize) -> Result<Fetched>;
}

/// Decode JSON payloads into a batch with `schema`. Unknown fields are ignored; missing fields
/// become NULL; tombstones (empty payloads) are skipped.
pub fn decode_json(schema: &SchemaRef, payloads: &[Option<Vec<u8>>], context: &str) -> Result<RecordBatch> {
    let mut decoder = ReaderBuilder::new(schema.clone())
        .with_batch_size(payloads.len().max(1))
        .build_decoder()?;
    for (i, p) in payloads.iter().enumerate() {
        let Some(p) = p else { continue };
        if p.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let mut line = p.clone();
        line.push(b'\n');
        let mut slice = &line[..];
        while !slice.is_empty() {
            let n = decoder
                .decode(slice)
                .map_err(|e| NodraError::Source(format!("{context}: message #{i} is not valid JSON for the declared schema: {e}")))?;
            if n == 0 {
                break;
            }
            slice = &slice[n..];
        }
    }
    match decoder.flush()? {
        Some(b) => Ok(b),
        None => Ok(RecordBatch::new_empty(schema.clone())),
    }
}

/// Shared handle to a stream source.
pub type StreamRef = Arc<dyn StreamSource>;
