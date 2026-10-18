//! Error type shared across Nodra.

use std::fmt;

/// Result alias used throughout the crate.
pub type Result<T, E = NodraError> = std::result::Result<T, E>;

/// All errors produced by Nodra.
#[derive(Debug, thiserror::Error)]
pub enum NodraError {
    /// The job file is malformed or uses an unsupported construct.
    #[error("job error: {0}")]
    Job(String),
    /// The planner rejected the job (class 5) or found an unsupported shape.
    #[error("plan rejected: {0}")]
    Rejected(Rejection),
    /// Invalid configuration (settings, CLI flags).
    #[error("configuration error: {0}")]
    Config(String),
    /// A Delta table is in a state Nodra cannot handle safely.
    #[error("delta error: {0}")]
    Delta(String),
    /// Another process committed progress for one of this worker's units.
    #[error("progress conflict: {0}")]
    ProgressConflict(String),
    /// Error from a stream source (Kafka, jsonl_stream).
    #[error("source error: {0}")]
    Source(String),
    /// DataFusion error.
    #[error(transparent)]
    DataFusion(#[from] datafusion::error::DataFusionError),
    /// Arrow error.
    #[error(transparent)]
    Arrow(#[from] datafusion::arrow::error::ArrowError),
    /// Parquet error.
    #[error(transparent)]
    Parquet(#[from] datafusion::parquet::errors::ParquetError),
    /// delta-rs error.
    #[error(transparent)]
    DeltaRs(#[from] deltalake_core::DeltaTableError),
    /// Object store error.
    #[error(transparent)]
    ObjectStore(#[from] object_store::Error),
    /// I/O error.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// JSON error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),
}

/// Why the planner refused a job. Rendered by `nodra plan --explain`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejection {
    /// Short description of the offending construct.
    pub reason: String,
    /// Suggestion for the user (e.g. "run this job on Spark or Sail").
    pub hint: String,
}

impl fmt::Display for Rejection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.reason, self.hint)
    }
}

impl NodraError {
    /// Shorthand for a job error.
    pub fn job(msg: impl Into<String>) -> Self {
        NodraError::Job(msg.into())
    }
    /// Shorthand for a configuration error.
    pub fn config(msg: impl Into<String>) -> Self {
        NodraError::Config(msg.into())
    }
    /// Shorthand for a Delta error.
    pub fn delta(msg: impl Into<String>) -> Self {
        NodraError::Delta(msg.into())
    }
    /// Shorthand for a rejection with a hint.
    pub fn rejected(reason: impl Into<String>, hint: impl Into<String>) -> Self {
        NodraError::Rejected(Rejection { reason: reason.into(), hint: hint.into() })
    }
}
