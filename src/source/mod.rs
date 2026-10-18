//! Sources: Delta tables and stream sources.

pub mod jsonl;
#[cfg(feature = "kafka")]
pub mod kafka;
pub mod stream;
