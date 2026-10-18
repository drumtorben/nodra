//! Nodra: a driverless, shuffle-free pipeline engine on DataFusion and Delta Lake.
//!
//! See `docs/design.md` for the execution model.
#![warn(missing_docs)]

pub mod cli;
pub mod delta;
pub mod error;
pub mod exec;
pub mod hash;
pub mod job;
pub mod merge;
pub mod metrics;
pub mod partial;
pub mod planner;
pub mod scan;
pub mod session;
pub mod settings;
pub mod sink;
pub mod source;
pub mod status;
pub mod types;

pub use error::{NodraError, Result};
