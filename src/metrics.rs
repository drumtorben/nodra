//! Minimal Prometheus text-format metrics, served over plain HTTP.
//!
//! Metrics are per worker (label `worker`). The endpoint is intentionally tiny: it answers every
//! request on the socket with the current metrics, which is all Prometheus needs.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Metrics of one worker.
#[derive(Debug, Default)]
pub struct WorkerMetrics {
    /// Batches committed.
    pub batches: AtomicU64,
    /// Input rows read.
    pub rows_in: AtomicU64,
    /// Rows written to the sink (or partial-state rows for class 3).
    pub rows_out: AtomicU64,
    /// Data files written.
    pub files_written: AtomicU64,
    /// Commits retried after losing a log-version race.
    pub commit_retries: AtomicU64,
    /// Batches abandoned because another process committed the same progress.
    pub progress_conflicts: AtomicU64,
    /// Duration of the last batch in milliseconds.
    pub last_batch_ms: AtomicU64,
    /// Input not yet processed: stream records behind the high watermark, or Delta versions.
    pub lag: AtomicU64,
    /// Unix time (seconds) of the last successful commit.
    pub last_commit_unix: AtomicU64,
}

type Registry = Mutex<BTreeMap<(String, u32), Arc<WorkerMetrics>>>;

fn registry() -> &'static Registry {
    static R: OnceLock<Registry> = OnceLock::new();
    R.get_or_init(Default::default)
}

/// Metrics handle for `job`/`worker`, created on first use.
pub fn worker(job: &str, worker: u32) -> Arc<WorkerMetrics> {
    registry()
        .lock()
        .expect("metrics registry poisoned")
        .entry((job.to_string(), worker))
        .or_default()
        .clone()
}

/// Render all metrics in Prometheus text format.
pub fn render() -> String {
    let reg = registry().lock().expect("metrics registry poisoned");
    let mut out = String::new();
    let series: [(&str, &str, &str, fn(&WorkerMetrics) -> u64); 9] = [
        ("nodra_batches_total", "counter", "Batches committed.", |m| m.batches.load(Ordering::Relaxed)),
        ("nodra_rows_in_total", "counter", "Input rows read.", |m| m.rows_in.load(Ordering::Relaxed)),
        ("nodra_rows_out_total", "counter", "Rows written.", |m| m.rows_out.load(Ordering::Relaxed)),
        ("nodra_files_written_total", "counter", "Data files written.", |m| m.files_written.load(Ordering::Relaxed)),
        ("nodra_commit_retries_total", "counter", "Commit retries after log-version races.", |m| m.commit_retries.load(Ordering::Relaxed)),
        ("nodra_progress_conflicts_total", "counter", "Batches abandoned due to concurrent progress.", |m| m.progress_conflicts.load(Ordering::Relaxed)),
        ("nodra_last_batch_milliseconds", "gauge", "Duration of the last batch.", |m| m.last_batch_ms.load(Ordering::Relaxed)),
        ("nodra_lag", "gauge", "Unprocessed input (records for streams, versions for Delta).", |m| m.lag.load(Ordering::Relaxed)),
        ("nodra_last_commit_timestamp_seconds", "gauge", "Unix time of the last commit.", |m| m.last_commit_unix.load(Ordering::Relaxed)),
    ];
    for (name, kind, help, get) in series {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} {kind}");
        for ((job, w), m) in reg.iter() {
            let _ = writeln!(out, "{name}{{job=\"{job}\",worker=\"{w}\"}} {}", get(m));
        }
    }
    out
}

/// Serve metrics on `addr` until the process exits.
pub async fn serve(addr: String) -> std::io::Result<()> {
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!(%addr, "serving metrics");
    loop {
        let (mut sock, _) = listener.accept().await?;
        tokio::spawn(async move {
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            let body = render();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                body.len()
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.shutdown().await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_labels() {
        let m = worker("unit-test-job", 3);
        m.batches.fetch_add(2, Ordering::Relaxed);
        let text = render();
        assert!(text.contains("nodra_batches_total{job=\"unit-test-job\",worker=\"3\"} 2"), "{text}");
        assert!(text.contains("# TYPE nodra_lag gauge"));
    }
}
