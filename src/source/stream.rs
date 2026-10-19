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
/// become NULL; tombstones (empty payloads) are skipped. The first message that does not match
/// the schema fails the whole batch (`context` and the message index are part of the error).
pub fn decode_json(schema: &SchemaRef, payloads: &[Option<Vec<u8>>], context: &str) -> Result<RecordBatch> {
    match decode_all(schema, payloads) {
        Ok(b) => Ok(b),
        Err(_) => {
            // Find the offending message for a precise error.
            for (i, p) in payloads.iter().enumerate() {
                if let Err(e) = decode_all(schema, std::slice::from_ref(p)) {
                    return Err(NodraError::Source(format!(
                        "{context}: message #{i} does not match the declared schema: {e}"
                    )));
                }
            }
            decode_all(schema, payloads)
        }
    }
}

/// Like [`decode_json`], but skips messages that do not match the schema and returns their
/// indexes and errors (`nodra.bad_messages = 'skip'`).
pub fn decode_json_lenient(
    schema: &SchemaRef,
    payloads: &[Option<Vec<u8>>],
) -> Result<(RecordBatch, Vec<(usize, String)>)> {
    if let Ok(b) = decode_all(schema, payloads) {
        return Ok((b, Vec::new()));
    }
    let mut good = Vec::with_capacity(payloads.len());
    let mut bad = Vec::new();
    for (i, p) in payloads.iter().enumerate() {
        match decode_all(schema, std::slice::from_ref(p)) {
            Ok(_) => good.push(p.clone()),
            Err(e) => bad.push((i, e.to_string())),
        }
    }
    Ok((decode_all(schema, &good)?, bad))
}

fn decode_all(schema: &SchemaRef, payloads: &[Option<Vec<u8>>]) -> Result<RecordBatch> {
    let mut decoder =
        ReaderBuilder::new(schema.clone()).with_batch_size(payloads.len().max(1)).build_decoder()?;
    for p in payloads {
        let Some(p) = p else { continue };
        if p.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        let mut line = p.clone();
        line.push(b'\n');
        let mut slice = &line[..];
        while !slice.is_empty() {
            let n = decoder.decode(slice)?;
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
