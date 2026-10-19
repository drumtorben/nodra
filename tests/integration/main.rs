//! Integration tests, in one binary (each test binary links all of DataFusion).
//!
//! Run a group with `cargo test --test integration <module>::`, e.g. `rescale::`.

mod common;
mod support;

mod classes;
mod cli;
mod differential;
#[cfg(feature = "kafka")]
mod kafka;
mod maintenance;
mod partitions;
mod pipeline;
mod planner_regressions;
mod planner_shapes;
mod progress;
mod rescale;
mod rescale_fencing;
mod rescale_generations;
mod robustness;
mod sink;
mod start_positions;
mod states;
mod stream;
mod zombies;
