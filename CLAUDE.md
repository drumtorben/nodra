# CLAUDE.md

Guidance for Claude Code (and humans) working on this repository.

## What Nodra is

A driverless, shuffle-free SQL pipeline engine on DataFusion 55 and delta-rs
(`deltalake-core` 1.1.2). A job is one SQL file (`SET …; INSERT INTO delta(…) SELECT …`).
Many identical worker processes each process their own slice of the input and commit results
*together with their progress* (Delta `txn` actions) to Delta tables; the Delta log is the only
coordination point. Read `README.md` for the overview, `docs/design.md` for the execution model
(the specification the code follows), `docs/jobs.md` for the job reference and
`docs/operations.md` for running it.

## Commands

```sh
cargo build                                   # default: local filesystem only
cargo build --features azure,kafka-ssl        # what production images use (see Dockerfile)
cargo test                                    # unit + integration tests, no external services
cargo test --features azure,kafka-ssl         # also Azure URL/credential and Kafka mock-cluster tests
cargo test --test integration rescale::       # one group of integration tests (module path)
NODRA_TEST_KAFKA=localhost:9092 cargo test --features kafka --test integration kafka::  # + real-broker tests
cargo test --lib planner                      # unit tests of one module
cargo fmt && cargo clippy --all-targets --features azure,kafka-ssl -- -D warnings
cargo run --example demo_data -- /tmp/nodra-demo && cargo run -- plan examples/jobs/daily_brand_stats.sql
cargo run --release --example bench -- /tmp/nodra-bench 8000000 16 2
```

Requirements: Rust ≥ 1.95 (deltalake-core needs it). The `kafka` feature builds librdkafka from
source (C toolchain, `make`, zlib headers); `kafka-ssl` additionally needs OpenSSL headers.
CI (`.github/workflows/ci.yml`) runs fmt, clippy with `-D warnings` and all tests with all
features — keep all three green.

## Code map

| Module | Responsibility | Start reading at |
| --- | --- | --- |
| `job` | parse job files (`SET`, one `INSERT`), source/sink calls, rewrite sources to `__nodra_s<i>` tables, canonical source keys | `Job::parse`, `SourceSpec::key`, `normalize_location` |
| `settings` | `nodra.*` settings (`KNOWN_KEYS`), DataFusion passthrough | `KNOWN_KEYS` |
| `planner` | logical plan, driving-source choice, classification (classes 1–4, rejections), merge scope, `nodra plan` text | `plan_job_with`, `Walk::visit` |
| `exec` | `Pipeline` (prepared job), worker loop, batches per class, progress cache, commits | `Pipeline::prepare`, `run_worker`, `Worker::step_*`, `commit_checked` |
| `rescale` | worker generations of Delta-driven jobs (changing `--workers`), start keys | `place`, `Handover::processed` |
| `partial` | split the physical plan at the aggregate (class 3): worker plan, canonical state layout, merge plan | `worker_plan`, `merge_plan`, `canonical_order` |
| `merge` | class-3 merge step: Final aggregate over partial states, replace (scoped) sink partitions | `merge` |
| `delta` | all Delta access: open, incremental commits, bulk `txn` reading, create, commit outcomes, Azure store factory | `commit`, `txn_map`, `changes` |
| `scan` | `FileListTable`: DataFusion table over an explicit file list, partition pruning | `FileListTable::scan` |
| `sink` | write Parquet files for a result stream, columns by name or position | `write_stream` |
| `session` | DataFusion session with Nodra's fixed options | `new_context` |
| `source` | `StreamSource` trait, Kafka (feature), `jsonl_stream`, JSON decoding | `stream.rs` |
| `types` | Arrow ↔ Delta type mapping, column-name checks, casts | `delta_type`, `cast_batch_by_name` |
| `status`, `metrics`, `cli`, `main` | `nodra status`, Prometheus endpoint, argument parsing, logging | |

## Invariants — do not break these

Nodra's value is exactly-once processing without coordination. Most past bugs were silent
duplicates or losses; every one of them has a regression test. Before changing the areas below,
read the relevant section of `docs/design.md`.

1. **Progress and data commit together.** A batch is computed from progress read at a read
   snapshot *S* and committed with *S* as delta-rs' read snapshot, so every concurrent commit
   after *S* is conflict-checked. Writing the same `txn` appId as a concurrent commit fails with
   `ConcurrentTransaction`; that is how zombie replicas are fenced (`CommitOutcome::Conflict`).
2. **The worker's progress cache must equal its read snapshot.** See `Worker::committed`,
   `adopt`, `verify`, `open_new_target`, and the replay of commits between read and commit
   version (`delta::txns_in_commits`) in `commit_checked`. A cache that misses another
   process's commit re-reads input that was already committed — a duplicate.
3. **`remove` actions are only valid on the snapshot they were computed from.** Commits with
   removes (merge step, `mode => 'overwrite'`) use `max_retries = 0` and recompute on a newer
   snapshot (`CommitOutcome::Stale`). delta-rs' own retries would accept, e.g., a concurrent
   OPTIMIZE (`dataChange = false` removes are ignored by its conflict checker).
4. **Progress keys are a persistent format.** Changing a key format (or `SourceSpec::key`)
   restarts every running job from scratch. Formats are listed in `docs/jobs.md`; keep both in
   sync, and treat changes like a breaking change.
5. **Class 3 state layout.** Partial states are stored in a canonical layout (group columns in
   logical order, identified by name — physical optimizers may reorder them), the partials table
   records the aggregation (`partial::LAYOUT_KEY`, `JobPlan::split_signature`), and `merge_plan`
   reproduces the split's output partitioning and ordering and runs `SanityCheckPlan`. The
   session fixes `enable_physical_uncorrelated_scalar_subquery = false` and
   `top_down_join_key_reordering = false` and removes the `aggregate_statistics` rule; do not
   undo that.
6. **Planner: reject rather than risk wrong results.** The driving source may be scanned only
   once; nothing below the split aggregate may need rows of other workers (LIMIT incl.
   `Sort { fetch }`, windows, unions, outer joins preserving a broadcast side, …); class 4
   allows only partition-local operators above the join. Once a job has run, its driving source
   and class stay what the progress keys say (`Previous`).
7. **Everyone starts from the same place.** First-generation workers share one snapshot/start
   version (`rescale::start_key`); stream jobs with `start = latest` record every partition's
   start offset (`rescale::stream_start_key`); rescaled generations skip what *any* earlier
   generation processed.
8. **Partition values are compared typed**, never as raw strings (`scan::canonical_partition_values`):
   other writers spell values differently.

## Testing

- **Unit tests** live in each module (`#[cfg(test)] mod tests`), e.g. planner classification,
  rescale placement, checkpoint selection.
- **Integration tests** are one binary, `tests/integration/main.rs`, with one module per area
  (`rescale`, `zombies`, `start_positions`, `maintenance`, `differential`, …). Add new modules
  there; do not add new top-level `tests/*.rs` files (each would link all of DataFusion again,
  ~400 MB per binary).
- **Differential testing is the main oracle**: run the job with N workers (often after several
  incremental runs) and compare with a single-node DataFusion run of the same query —
  `common::assert_matches_reference`, or `support::planner::outcome` for "rejected or correct".
- **Concurrency**: `exec::testing::fail_before_commit` injects crashes before a commit; a FIFO
  as a `jsonl_stream` partition pauses a worker deterministically inside a batch
  (`tests/integration/zombies.rs`); `common::compact` simulates OPTIMIZE.
- When you fix a correctness bug, first write the failing test that shows the wrong rows, then
  fix. Timing-sensitive tests should pass repeatedly (`for i in 1 2 3; do cargo test …; done`).

## Conventions

- `rustfmt.toml`: `max_width = 110`, `use_small_heuristics = "Max"`. Very long string literals
  make rustfmt give up on an expression; split them.
- `#![warn(missing_docs)]`: every public item has a doc comment. Comments explain *why*.
- Errors: `NodraError` helpers (`job`, `config`, `delta`, `rejected(reason, hint)`). A rejection
  always carries a hint the user can act on. Never panic in library code.
- Logging: `tracing`; Nodra's level from `RUST_LOG`, libraries at `warn` (see `main.rs`).
- New `nodra.*` setting: add it to `settings::KNOWN_KEYS` (with default and description), an
  accessor, and a row in the settings table of `docs/jobs.md` — a unit test checks the table.
- New behavior or guarantee: update `docs/design.md` (and `docs/operations.md` if operators must
  know). Keep `README.md`'s limitations honest.

## Dependency notes

- delta-rs is used without its `datafusion` feature: Nodra cannot *write* tables whose protocol
  requires invariants, check constraints, generated columns, CDF or column mapping, and refuses
  to read tables with deletion vectors or column mapping (`delta::check_readable`).
- `object_store::parse_url_opts` ignores `AZURE_*` environment variables; the Azure factory in
  `delta.rs` builds stores with `MicrosoftAzureBuilder::from_env()`.
- delta-kernel logs every error it returns; `main.rs` silences the `buoyant_kernel` target.
- `Cargo.lock` is resolved against crates.io. The project was first built in an environment
  without crates.io, from the same versions' GitHub sources (except `buoyant_kernel_engine`:
  0.28.0 on crates.io vs. the 0.28.1 sources used there). Run the full test suite after the first
  build from crates.io.

## Open ends (good next steps)

- Run against a real Kafka cluster and a real ADLS Gen2 account (only mocks/unit tests so far).
- Schema evolution of the sink; Avro / schema registry for Kafka.
- Compaction of partial states (DataFusion `PartialReduce`), Delta file statistics for pruning,
  caching broadcast tables between batches.
- Deletion vectors in sources; class 4 picking up late files of processed partitions.
