//! `jsonl_stream('<dir>')`: a directory of `<partition>.jsonl` files where each line is a
//! message and its 0-based line number is its offset. It behaves like a Kafka topic without
//! a broker and is used for tests and local demos.

use std::path::PathBuf;

use async_trait::async_trait;

use super::stream::{Fetched, StreamSource};
use crate::error::{NodraError, Result};

/// See module docs.
#[derive(Debug, Clone)]
pub struct JsonlStream {
    dir: PathBuf,
}

impl JsonlStream {
    /// Open a stream directory (it must exist).
    pub fn new(dir: impl Into<PathBuf>) -> Result<Self> {
        let dir = dir.into();
        if !dir.is_dir() {
            return Err(NodraError::Source(format!("jsonl_stream: {} is not a directory", dir.display())));
        }
        Ok(Self { dir })
    }

    fn file(&self, partition: i32) -> PathBuf {
        self.dir.join(format!("{partition}.jsonl"))
    }

    async fn lines(&self, partition: i32) -> Result<Vec<Vec<u8>>> {
        let path = self.file(partition);
        let data = match tokio::fs::read(&path).await {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        // Only complete lines count: a writer may be appending the last one.
        let complete = match data.iter().rposition(|b| *b == b'\n') {
            Some(p) => &data[..=p],
            None => &data[..0],
        };
        Ok(complete
            .split(|b| *b == b'\n')
            .take(complete.iter().filter(|b| **b == b'\n').count())
            .map(<[u8]>::to_vec)
            .collect())
    }
}

#[async_trait]
impl StreamSource for JsonlStream {
    async fn partitions(&self) -> Result<Vec<i32>> {
        let mut parts = Vec::new();
        let mut rd = tokio::fs::read_dir(&self.dir).await?;
        while let Some(e) = rd.next_entry().await? {
            let name = e.file_name().to_string_lossy().to_string();
            if let Some(stem) = name.strip_suffix(".jsonl") {
                if let Ok(p) = stem.parse::<i32>() {
                    parts.push(p);
                }
            }
        }
        parts.sort_unstable();
        if parts.is_empty() {
            return Err(NodraError::Source(format!(
                "jsonl_stream: no <partition>.jsonl files in {}",
                self.dir.display()
            )));
        }
        Ok(parts)
    }

    async fn watermarks(&self, partition: i32) -> Result<(i64, i64)> {
        Ok((0, self.lines(partition).await?.len() as i64))
    }

    async fn fetch(&self, partition: i32, from: i64, until: i64, max: usize) -> Result<Fetched> {
        let lines = self.lines(partition).await?;
        let start = from.max(0) as usize;
        let end = (until.max(0) as usize).min(lines.len()).min(start.saturating_add(max));
        let payloads =
            if start < end { lines[start..end].iter().cloned().map(Some).collect() } else { Vec::new() };
        Ok(Fetched { payloads, first_offset: from, next_offset: end.max(start) as i64 })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    use super::*;
    use crate::source::stream::decode_json;

    #[tokio::test]
    async fn reads_complete_lines_only() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("0.jsonl"), "{\"a\":1}\n{\"a\":2}\n{\"a\":").unwrap();
        std::fs::write(dir.path().join("1.jsonl"), "").unwrap();
        let s = JsonlStream::new(dir.path()).unwrap();
        assert_eq!(s.partitions().await.unwrap(), vec![0, 1]);
        assert_eq!(s.watermarks(0).await.unwrap(), (0, 2));
        let f = s.fetch(0, 1, 2, 10).await.unwrap();
        assert_eq!(f.payloads.len(), 1);
        assert_eq!(f.next_offset, 2);
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let b = decode_json(&schema, &f.payloads, "t").unwrap();
        assert_eq!(b.num_rows(), 1);
        let f = s.fetch(0, 0, 2, 1).await.unwrap();
        assert_eq!((f.payloads.len(), f.next_offset), (1, 1));
    }
}
