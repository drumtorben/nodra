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
> classes are implemented and tested (133 tests: crash, zombie-replica, rescaling,
> concurrent-maintenance, Kafka mock-cluster and differential tests against single-node DataFusion
> runs; three independent review rounds). It has not
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

## Performance

`bench/run.sh` runs two jobs with Nodra and with Spark 4 (local mode) on the same Delta input,
on the same machine with the same parallelism, and checks that both produce the same results:

* **Enrichment** (class 2): join every event with a 1 000-row campaign table, write a Delta
  table partitioned by day.
* **Daily aggregation** (class 3): join, then `count(*)`, `sum` and `approx_distinct` per day
  and brand (partial states + merge step).

One laptop (Apple M4 Pro, 14 cores, 48 GB), Nodra with 14 local workers vs. Spark 4.0.4
`local[14]` with Delta 4.0.1; median of 3 runs after a warm-up, Spark in a fresh JVM per run
like a scheduled job. Results matched in every run.

| | 40 M events (648 MB) | | 400 M events (5.3 GB) | |
| --- | ---: | ---: | ---: | ---: |
| | Nodra | Spark | Nodra | Spark |
| Enrichment | 1.4 s | 9.2 s | 14.3 s | 48.5 s |
| Daily aggregation | 0.5 s | 3.1 s | 3.6 s | 11.5 s |
| Both, incl. Spark startup (~2 s) | 1.9 s | 14.3 s | 17.9 s | 62.0 s |
| Peak memory (16 GB Spark heap) | 2.7 GB | 13.3 GB | 6.4 GB | 17.7 GB |
| Peak memory (4 GB Spark heap) | 2.6 GB | 5.0 GB | 6.4 GB | 5.1 GB |

With a 4 GB heap Spark runs slightly slower (56.0 s / 12.2 s at 400 M events). Most of Nodra's
memory comes from every local worker using all cores (`datafusion.execution.target_partitions`
defaults to the CPU count): with `SET datafusion.execution.target_partitions = 4` the 400 M run
peaks at 3.9 GB at the same speed; with `= 1` at 2.8 GB, with a 25 % slower aggregation.

What this does and does not show: both engines run on one machine and read and write local
files, and both jobs need no shuffle — exactly the jobs Nodra is for. It says nothing about jobs
that do need a shuffle (Nodra rejects them), and nothing about clusters or object storage yet.

```sh
bench/run.sh                          # 40 M events, all cores, 3 runs; needs cargo, uv, Java 17+
ROWS=400000000 bench/run.sh           # bigger (input generated once; ~33 GB on disk incl. outputs)
SPARK_MEM=4g bench/run.sh             # Spark driver heap (default 16g)
```

## Documentation

* [docs/jobs.md](docs/jobs.md) — job file reference: sources, sinks, settings
* [docs/operations.md](docs/operations.md) — running on Kubernetes, Azure, Kafka, monitoring, rescaling
* [docs/design.md](docs/design.md) — execution model and exactly-once design
* [examples/jobs](examples/jobs) — example jobs for every class
* [CLAUDE.md](CLAUDE.md) — contributor guide: code map, invariants, testing conventions (also read by Claude Code)

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
  `kafka-ssl`) feature; it is tested against librdkafka's mock cluster, not a real broker.
* **Azure** (`azure` feature): URL and credential handling is unit-tested, but Nodra has not
  run against a real storage account yet.
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

Integration tests are one binary (`cargo test --test integration rescale::` runs a group).
`Cargo.lock` is resolved against crates.io. The code was developed in an environment without
crates.io access, against the same crate versions built from their GitHub sources; the one
exception is `buoyant_kernel_engine`, where crates.io has 0.28.0 and the tested build used the
0.28.1 sources (buoyant-data/delta-kernel-rs, commit `ab66589`).

`jsonl_stream('<dir>')` is a broker-free stand-in for Kafka (one file per partition, one
message per line) used by the tests and the demo.

## License

Apache-2.0
