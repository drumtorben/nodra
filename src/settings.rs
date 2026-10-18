//! Job settings from `SET key = value;` statements and CLI overrides.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::error::{NodraError, Result};

/// Known `nodra.*` keys with their defaults (rendered by `docs/jobs.md`).
pub const KNOWN_KEYS: &[(&str, &str, &str)] = &[
    ("nodra.job_name", "file stem", "Name used in progress keys; changing it restarts the job from scratch."),
    ("nodra.trigger", "60s", "Pause between micro-batches in streaming mode."),
    ("nodra.broadcast_threshold", "1GiB", "Delta tables up to this size are broadcast to every worker."),
    ("nodra.max_files_per_batch", "1000", "Maximum number of new Delta files a worker reads per batch."),
    ("nodra.max_records_per_partition", "100000", "Maximum stream records read per partition per batch."),
    ("nodra.ignore_changes", "false", "Skip deletes/updates in Delta sources instead of failing."),
    ("nodra.start", "earliest", "Where a source starts without progress: `earliest` or `latest`."),
    ("nodra.merge_interval", "60s", "How often worker 0 runs the merge step of a class-3 job (0 = after every batch)."),
    ("nodra.partials_location", "<sink>__nodra_partials", "Delta table that stores partial aggregate states."),
    ("nodra.commit_retries", "50", "How often a commit that lost a log-version race is retried."),
    ("nodra.bad_messages", "fail", "Stream messages that do not match the schema: `fail` the job or `skip` (logged and counted)."),
];

/// Parsed settings. Unknown `nodra.*` keys are rejected so typos surface early; other keys
/// (e.g. `datafusion.execution.batch_size`) are passed to DataFusion unchanged.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Settings {
    values: BTreeMap<String, String>,
}

impl Settings {
    /// Set a key, validating `nodra.*` keys.
    pub fn set(&mut self, key: &str, value: &str) -> Result<()> {
        let key = key.trim().to_ascii_lowercase();
        if key.starts_with("nodra.") && !KNOWN_KEYS.iter().any(|(k, _, _)| *k == key) {
            return Err(NodraError::config(format!("unknown setting `{key}`")));
        }
        self.values.insert(key, value.trim().to_string());
        Ok(())
    }

    /// Raw value of a key.
    pub fn get(&self, key: &str) -> Option<&str> {
        self.values.get(key).map(String::as_str)
    }

    /// All non-`nodra.*` settings, forwarded to DataFusion's `SessionConfig`.
    pub fn engine_options(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .filter(|(k, _)| !k.starts_with("nodra."))
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// All `nodra.*` settings that were set explicitly.
    pub fn nodra_options(&self) -> impl Iterator<Item = (&str, &str)> {
        self.values
            .iter()
            .filter(|(k, _)| k.starts_with("nodra."))
            .map(|(k, v)| (k.as_str(), v.as_str()))
    }

    /// `nodra.trigger`.
    pub fn trigger(&self) -> Result<Duration> {
        self.duration("nodra.trigger", Duration::from_secs(60))
    }

    /// `nodra.merge_interval`.
    pub fn merge_interval(&self) -> Result<Duration> {
        self.duration("nodra.merge_interval", Duration::from_secs(60))
    }

    /// `nodra.broadcast_threshold` in bytes.
    pub fn broadcast_threshold(&self) -> Result<u64> {
        match self.get("nodra.broadcast_threshold") {
            None => Ok(1 << 30),
            Some(v) => parse_bytes(v),
        }
    }

    /// `nodra.max_files_per_batch`.
    pub fn max_files_per_batch(&self) -> Result<usize> {
        self.positive("nodra.max_files_per_batch", 1000)
    }

    /// `nodra.max_records_per_partition`.
    pub fn max_records_per_partition(&self) -> Result<usize> {
        self.positive("nodra.max_records_per_partition", 100_000)
    }

    /// `nodra.commit_retries`.
    pub fn commit_retries(&self) -> Result<usize> {
        self.positive("nodra.commit_retries", 50)
    }

    /// `nodra.ignore_changes`.
    pub fn ignore_changes(&self) -> Result<bool> {
        self.boolean("nodra.ignore_changes", false)
    }

    /// `nodra.bad_messages`: true if malformed stream messages are skipped.
    pub fn skip_bad_messages(&self) -> Result<bool> {
        match self.get("nodra.bad_messages").map(|s| s.to_ascii_lowercase()) {
            None => Ok(false),
            Some(s) if s == "fail" => Ok(false),
            Some(s) if s == "skip" => Ok(true),
            Some(s) => Err(NodraError::config(format!("nodra.bad_messages must be `fail` or `skip`, got `{s}`"))),
        }
    }

    /// `nodra.start`.
    pub fn start(&self) -> Result<StartPosition> {
        match self.get("nodra.start").map(|s| s.to_ascii_lowercase()) {
            None => Ok(StartPosition::Earliest),
            Some(s) if s == "earliest" => Ok(StartPosition::Earliest),
            Some(s) if s == "latest" => Ok(StartPosition::Latest),
            Some(s) => Err(NodraError::config(format!(
                "nodra.start must be `earliest` or `latest`, got `{s}`"
            ))),
        }
    }

    fn duration(&self, key: &str, default: Duration) -> Result<Duration> {
        match self.get(key) {
            None => Ok(default),
            Some(v) => parse_duration(v).map_err(|e| NodraError::config(format!("{key}: {e}"))),
        }
    }

    fn positive(&self, key: &str, default: usize) -> Result<usize> {
        match self.get(key) {
            None => Ok(default),
            Some(v) => match v.parse::<usize>() {
                Ok(n) if n > 0 => Ok(n),
                _ => Err(NodraError::config(format!("{key} must be a positive integer, got `{v}`"))),
            },
        }
    }

    fn boolean(&self, key: &str, default: bool) -> Result<bool> {
        match self.get(key).map(|s| s.to_ascii_lowercase()) {
            None => Ok(default),
            Some(v) if matches!(v.as_str(), "true" | "1" | "yes" | "on") => Ok(true),
            Some(v) if matches!(v.as_str(), "false" | "0" | "no" | "off") => Ok(false),
            Some(v) => Err(NodraError::config(format!("{key} must be a boolean, got `{v}`"))),
        }
    }
}

/// Where a source starts when no progress has been committed yet.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartPosition {
    /// Process all existing data.
    Earliest,
    /// Only process data that arrives after the first batch.
    Latest,
}

/// Parse `500ms`, `30s`, `5m`, `1h` or a plain number of seconds.
pub fn parse_duration(v: &str) -> std::result::Result<Duration, String> {
    let v = v.trim().to_ascii_lowercase();
    let (num, mult_ms) = if let Some(n) = v.strip_suffix("ms") {
        (n, 1u64)
    } else if let Some(n) = v.strip_suffix('s') {
        (n, 1_000)
    } else if let Some(n) = v.strip_suffix('m') {
        (n, 60_000)
    } else if let Some(n) = v.strip_suffix('h') {
        (n, 3_600_000)
    } else {
        (v.as_str(), 1_000)
    };
    let n: u64 = num.trim().parse().map_err(|_| format!("invalid duration `{v}`"))?;
    Ok(Duration::from_millis(n * mult_ms))
}

/// Parse a byte size: `1024`, `512KB`, `256MB`, `1GB`, `1GiB` (all binary multiples).
pub fn parse_bytes(v: &str) -> Result<u64> {
    let s = v.trim().to_ascii_lowercase().replace(' ', "");
    let units: [(&str, u64); 8] = [
        ("kib", 1 << 10),
        ("mib", 1 << 20),
        ("gib", 1 << 30),
        ("tib", 1 << 40),
        ("kb", 1 << 10),
        ("mb", 1 << 20),
        ("gb", 1 << 30),
        ("tb", 1 << 40),
    ];
    for (suffix, mult) in units {
        if let Some(n) = s.strip_suffix(suffix) {
            return n
                .parse::<u64>()
                .map(|n| n * mult)
                .map_err(|_| NodraError::config(format!("invalid size `{v}`")));
        }
    }
    s.strip_suffix('b')
        .unwrap_or(&s)
        .parse::<u64>()
        .map_err(|_| NodraError::config(format!("invalid size `{v}`")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("500ms").unwrap(), Duration::from_millis(500));
        assert_eq!(parse_duration("30s").unwrap(), Duration::from_secs(30));
        assert_eq!(parse_duration("5m").unwrap(), Duration::from_secs(300));
        assert_eq!(parse_duration("2").unwrap(), Duration::from_secs(2));
        assert!(parse_duration("abc").is_err());
    }

    #[test]
    fn bytes() {
        assert_eq!(parse_bytes("1GiB").unwrap(), 1 << 30);
        assert_eq!(parse_bytes("512MB").unwrap(), 512 << 20);
        assert_eq!(parse_bytes("1024").unwrap(), 1024);
        assert!(parse_bytes("lots").is_err());
    }

    #[test]
    fn unknown_nodra_key_is_rejected() {
        let mut s = Settings::default();
        assert!(s.set("nodra.trigerr", "1s").is_err());
        s.set("datafusion.execution.batch_size", "4096").unwrap();
        assert_eq!(s.engine_options().count(), 1);
    }
}
