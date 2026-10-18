# Nodra design

Nodra runs SQL pipelines on many independent workers **without a driver and without shuffle**.
Workers coordinate only through the transaction log of the Delta tables they write.
This document is the specification the implementation follows.

## 1. Jobs

A job is a single `.sql` file:

```sql
SET nodra.trigger = '60s';                 -- optional settings
INSERT INTO delta('/lake/agg/hourly', partition_by => 'day')
SELECT date_trunc('hour', e.ts) AS hour, CAST(e.ts AS DATE) AS day, c.brand, count(*) AS requests
FROM kafka('events', brokers => 'kafka:9092', format => 'json',
           schema => 'ts TIMESTAMP, campaign_id BIGINT, user_id VARCHAR') e
JOIN delta('/lake/dim/campaigns') c USING (campaign_id)
GROUP BY 1, 2, 3;
```

* `SET key = value;` statements configure the job (`nodra.*` keys, see `docs/jobs.md`).
* Exactly one `INSERT INTO <sink>(...) <query>` statement.
* Sources and sinks are table functions. Nodra rewrites every source call in the query
  into a registered table and hands the rewritten query to DataFusion.

## 2. Job classes

The planner (`nodra plan --explain`) assigns every job to a class. All supported classes
run without moving rows between workers.

| Class | Shape | Execution |
| --- | --- | --- |
| 1 · partition-parallel | one driving source, row-wise operators | each worker processes its own work units |
| 2 · broadcast join | driving source joined with small tables | small tables are loaded fully by every worker |
| 3 · mergeable aggregation | `GROUP BY`/`DISTINCT` over the driving source | workers persist *partial aggregate states*; a merge step finalizes them |
| 4 · co-partitioned join | two large Delta tables partitioned and joined on the same columns | a worker joins matching partitions of both tables locally |
| 5 · shuffle | anything else | rejected, with the reason |

**Driving source.** The source that is split across workers: the stream source if there is one
(at most one), otherwise the largest Delta table. Every other Delta table must be *broadcastable*
(size ≤ `nodra.broadcast_threshold`, default 1 GiB) or co-partitioned with the driving table.

**Rejected shapes** (class 5): stream–stream joins, joins of large non-co-partitioned tables,
outer joins that preserve the broadcast side, window functions, `LIMIT`, nested aggregations,
`UNION` involving the driving source, scalar subqueries over the driving source.

## 3. Work units and assignment

Workers are numbered `0..n` (`--worker i --workers n`; in Kubernetes the StatefulSet ordinal).
Assignment is a pure function, so workers never talk to each other.

* **Delta source:** a unit is a data file (class 1–3) or a partition (aligned aggregation,
  class 4). `unit → worker = fnv1a64(key) mod n`.
* **Stream source (Kafka / `jsonl_stream`):** a unit is a partition. `partition → worker = p mod n`.

## 4. Incremental processing and exactly-once

Every batch is processed as

1. read progress for the worker's units from the **sink's** Delta log (`txn` actions),
2. determine the input after that progress,
3. execute the query on that input with DataFusion,
4. write Parquet data files into the sink table directory,
5. commit `add` actions **and** the new progress (`txn` actions) in one Delta commit.

Because data and progress land atomically, a crash at any point either loses the uncommitted
files (orphans, removed by `VACUUM`) or keeps both; a restarted worker resumes exactly after the
last committed progress.

Progress keys (`txn.appId`):

| Source | appId | version |
| --- | --- | --- |
| Delta, incremental | `nodra/<job>/<source>/w<i>of<n>` | last fully processed source table version |
| Stream partition | `nodra/<job>/<source>/p<partition>` | next offset to read |
| Class 4 partition | `nodra/<job>/part/<fnv(partition)>` | 1 = done |
| Merge | `nodra/<job>/merge` | partials table version merged |

**Commit conflicts.** Workers commit concurrently. Nodra retries a commit that lost the race for a
log version, but before each retry it re-reads the sink's `txn` versions: if another process
(for example a zombie replica of the same worker) has already committed progress for one of the
units, the batch is abandoned and the worker re-synchronizes. Progress for a unit can therefore
only ever move forward once per input range.

**Delta sources.** Incremental input is read from the source's commit files
(`_delta_log/<version>.json`). `add` actions with `dataChange = false` (OPTIMIZE/compaction
rewrites) are skipped. `remove` actions with `dataChange = true` (deletes/updates) fail the batch
unless `nodra.ignore_changes = true`, matching Spark's streaming semantics. If the needed commit
files were already cleaned up, the worker stops with an error instead of guessing.

**Rescaling.** Stream progress is per partition, so the worker count can change freely.
Delta-source progress is per worker; changing `n` requires that all old workers reached the same
source version (`nodra status` shows it); new workers then start from that version.

## 5. Mergeable aggregation (class 3)

Nodra uses DataFusion's two-phase aggregation:

* Workers execute the physical plan up to the **Partial** `AggregateExec` and write the
  partial-state batches into a *partials* Delta table (`<sink>__nodra_partials`), committed with
  the worker's progress exactly as above. State columns are stored under positional names
  (`s0, s1, …`) because DataFusion state names are not valid Delta column names.
* The merge step (`nodra merge`, or worker 0 with `nodra.merge_interval`) builds the **Final**
  `AggregateExec` with the same group/aggregate expressions over a scan of the partials, applies
  everything above the aggregate (projection, `HAVING`, …) and replaces the affected sink data.
* **Partition-scoped merge:** if the sink is partitioned (`partition_by`) by group keys, only sink
  partitions that received new partials since the last merge are recomputed and replaced. An
  unpartitioned sink is recomputed fully.
* Every DataFusion aggregate with mergeable state works: `count`, `sum`, `avg`, `min`, `max`,
  `approx_distinct` (HyperLogLog), `count(DISTINCT …)`, statistical aggregates, …

The merge commit carries `nodra/<job>/merge = <partials version>`, so merging is idempotent.

## 6. Co-partitioned joins (class 4)

Two Delta tables partitioned by the same columns and joined with equality on all of them are
processed per partition: worker `fnv(partition values) mod n` joins the files of that partition
from both tables. Class 4 runs in batch mode (`--once`): each partition is processed once and
marked with a `txn` entry.

## 7. Components

| Module | Responsibility |
| --- | --- |
| `job` | parse job files, settings, sink and source calls; rewrite the query |
| `planner` | build the logical plan, pick the driving source, classify, explain |
| `delta` | Delta access: snapshot, incremental commits, create, commit with `txn` |
| `scan` | `TableProvider` over an explicit file list with partition values |
| `source` | Delta, Kafka (feature `kafka`) and `jsonl_stream` sources |
| `exec` | the worker loop: batches, progress, retries, shutdown |
| `sink` | write Parquet files and build `add` actions |
| `partial` / `merge` | split aggregations and run the merge step |
| `metrics` | Prometheus text endpoint |
