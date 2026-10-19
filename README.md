# Nodra

**Driverless, shuffle-free SQL pipelines on DataFusion and Delta Lake.**

Nodra runs a SQL job on any number of independent workers. There is no driver, no cluster
manager and no shuffle: each worker reads its own slice of the input, joins it with small
tables that every worker loads, and commits results *together with its progress* to Delta Lake.
The Delta transaction log is the only coordination point, which also makes processing
exactly-once.

Many pipelines that run on Spark only because Spark is the default tool never exchange rows
between partitions: ingest, filtering, enrichment with dimension tables, time-bucketed
aggregations. Nodra runs exactly those, as a single binary per worker, and tells you when a job
needs a real shuffle engine.

> Status: **v0.1, early**. The execution model, exactly-once handling, rescaling and all job
> classes are implemented and tested (TESTCOUNT tests: crash, zombie-replica, rescaling,
> concurrent-maintenance and differential tests against single-node DataFusion runs). It has not
> run in production yet. See [Limitations](#limitations).

## Quickstart

```sh
cargo build --release                        # add --features azure,kafka-ssl as needed
cargo run --release --example demo_data -- /tmp/nodra-demo

./target/release/nodra plan examples/jobs/daily_brand_stats.sql
./target/release/nodra run  examples/jobs/daily_brand_stats.sql --once --local-workers 4
./target/release/nodra status examples/jobs/daily_brand_stats.sql
```

A job is one SQL file. Sources and sinks are table functions:

```sql
INSERT INTO delta('/tmp/nodra-demo/gold/daily_brand_stats', partition_by => 'day')
SELECT e.day,
       c.brand,
       count(*)                   AS events,
       sum(c.cpm) / 1000          AS revenue,
       approx_distinct(e.user_id) AS users
FROM delta('/tmp/nodra-demo/bronze/events') e
JOIN delta('/tmp/nodra-demo/dim/campaigns') c ON e.campaign_id = c.campaign_id
GROUP BY e.day, c.brand;
```

`nodra plan` shows how the job will run:

```text
job:     daily_brand_stats
class:   3 · mergeable aggregation
sources:
  __nodra_s0  delta('/tmp/nodra-demo/bronze/events') (212.9 KiB, 9 files)
      driving · files assigned by hash
  __nodra_s1  delta('/tmp/nodra-demo/dim/campaigns') (1.9 KiB, 1 file)
      broadcast · loaded by every worker
sink:    delta('/tmp/nodra-demo/gold/daily_brand_stats') partitioned by [day]
output:
  day      Utf8
  brand    Utf8
  events   Int64
  revenue  Float64
  users    UInt64  (stored as Int64)
merge:   partial states → /tmp/nodra-demo/gold/daily_brand_stats__nodra_partials; recompute changed sink partitions [day]
```

Without `--once`, workers keep running and process new input every `nodra.trigger`.

## Job classes

The planner sorts every job into a class. Classes 1–4 run without moving rows between workers;
class 5 is rejected with the reason.

| Class | Shape | How it runs |
| --- | --- | --- |
| 1 · partition-parallel | one driving source, row-wise operators | each worker processes its own files or stream partitions |
| 2 · broadcast join | driving source joined with small tables | small tables (≤ `nodra.broadcast_threshold`) are loaded by every worker |
| 3 · mergeable aggregation | `GROUP BY` / `DISTINCT` over the driving source | workers persist DataFusion *partial aggregate states*; a merge step finalizes them and replaces only the sink partitions that changed |
| 4 · co-partitioned join | two large Delta tables partitioned and joined on the same columns | each worker joins whole partitions locally, every partition once |
| 5 · shuffle | everything else | rejected: window functions, `LIMIT`, outer joins preserving the broadcast side, stream–stream joins, … |

Class 3 works for every DataFusion aggregate with mergeable state: `count`, `sum`, `avg`, `min`,
`max`, `count(DISTINCT …)`, `approx_distinct` (HyperLogLog), statistical aggregates and more.

## How it works

```text
            ┌──────────── worker 0 ────────────┐
 Kafka /    │ read progress (txn) ─► read input │      ┌──────────────────────────┐
 Delta ───► │ DataFusion ─► Parquet files       │ ───► │ sink Delta table         │
 source     │ commit: files + progress, atomic  │      │  add actions + txn(appId)│
            └───────────────────────────────────┘      └──────────────────────────┘
            worker 1 … worker N: same, other slices      (class 3: partials table,
                                                          then merge into the sink)
```

* **Assignment is a pure function.** Stream partition `p` belongs to worker `p mod N`; a Delta
  file to worker `fnv1a(path) mod N`. Workers never talk to each other. In Kubernetes the worker
  index is the StatefulSet ordinal (`--worker auto`). Changing `N` hands over exactly where
  every worker stopped.
* **Exactly-once.** Data files and the new progress (Delta `txn` actions, e.g. the next Kafka
  offset per partition) are committed in one Delta commit. A crash before the commit loses only
  uncommitted files (cleaned up by `VACUUM`); a restarted worker resumes after the last commit.
  If a zombie replica of the same worker commits the same progress first, the second commit
  fails Delta's conflict check and is abandoned.
* **Incremental Delta input** is read from the source's commit files: compaction rewrites
  (`dataChange = false`, e.g. nightly `OPTIMIZE`) are skipped, deletes/updates stop the job
  unless `nodra.ignore_changes = true` (like Spark Structured Streaming).
* **Class 3** splits DataFusion's physical plan at the aggregate next to the driving source:
  workers run it in `Partial` mode, the merge step runs a `Final` aggregate over the persisted
  states plus everything above it.

Details: [docs/design.md](docs/design.md).

## Documentation

* [docs/jobs.md](docs/jobs.md) — job file reference: sources, sinks, settings
* [docs/operations.md](docs/operations.md) — running on Kubernetes, Azure, Kafka, monitoring, rescaling
* [docs/design.md](docs/design.md) — execution model and exactly-once design
* [examples/jobs](examples/jobs) — example jobs for every class

## Command line

```text
nodra run <job.sql> [--worker <i|auto>] [--workers <n>] [--once] [--set k=v]...
                    [--local-workers <n>] [--metrics-addr <host:port>]
nodra plan <job.sql> [--set k=v]...
nodra merge <job.sql> [--once]
nodra status <job.sql>
```

## Limitations

v0.1 deliberately does less than it eventually should:

* **Delta features:** tables with deletion vectors or column mapping are refused. Without the
  delta-rs `datafusion` feature, Nodra can only *write* tables with writer version 2 or writer
  version 7 limited to appendOnly/timestampNtz/v2Checkpoint/deletionVectors/... features (tables
  Nodra creates are fine).
* **Sink schema** must match the query output exactly; no schema evolution yet.
* **Stream formats:** JSON only (no Avro / schema registry yet). Kafka needs the `kafka` (or
  `kafka-ssl`) feature and has been tested against configuration only, not a live broker.
* **Rescaling** a *Delta-driven* job needs all old workers stopped (stream jobs rescale freely,
  also during rolling updates).
* **Class 4** processes each partition once; data arriving later in an already processed
  partition is ignored (with a warning).
* **Broadcast tables** are read as of each batch; rows processed earlier are not revisited when
  a dimension table changes (like Spark's stream–static joins).
* **Partial states are never compacted** yet (DataFusion's `PartialReduce` mode makes this
  possible); with partition-scoped merges this matters only for partitions that keep changing.
* Broadcast tables are reloaded every batch; Delta file statistics are not used for pruning
  yet (partition values and Parquet statistics are).

## Development

```sh
cargo test                               # unit + integration tests (no external services)
cargo test --features azure,kafka-ssl    # also compiles the Azure store and the Kafka source
```

`jsonl_stream('<dir>')` is a broker-free stand-in for Kafka (one file per partition, one
message per line) used by the tests and the demo.

## License

Apache-2.0
