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

The planner (`nodra plan`) assigns every job to a class. All supported classes
run without moving rows between workers.

| Class | Shape | Execution |
| --- | --- | --- |
| 1 · partition-parallel | one driving source, row-wise operators | each worker processes its own work units |
| 2 · broadcast join | driving source joined with small tables | small tables are loaded fully by every worker |
| 3 · mergeable aggregation | `GROUP BY`/`DISTINCT` over the driving source | workers persist *partial aggregate states*; a merge step finalizes them |
| 4 · co-partitioned join | two large Delta tables partitioned and joined on the same columns | a worker joins matching partitions of both tables locally |
| 5 · shuffle | anything else | rejected, with the reason |

**Driving source.** The source that is split across workers: the stream source if there is one
(at most one), otherwise the largest Delta table that can be split — not one marked
`broadcast => true`, not one on the non-preserved side of an outer, semi or anti join, and not
one read only in a subquery. Once a job has run, the driving source — and the class — stay what
the progress keys in the target show, even if another table has grown larger since: a broadcast
table that outgrows `nodra.broadcast_threshold` stops the job with a hint instead of silently
turning it into a class-4 job that would process everything again. Every other Delta table must be
*broadcastable* (size ≤ `nodra.broadcast_threshold`, default 1 GiB) or co-partitioned with the
driving table.

The driving source may be read only once in the plan: a self-join, a CTE used twice, a subquery
over it or a second source call on the same table would see only the worker's share of it (or,
as a broadcast copy, all of it on every worker).

**Rejected shapes** (class 5): stream–stream joins, joins of large non-co-partitioned tables,
outer joins that preserve the broadcast side, window functions, `LIMIT` (including
`ORDER BY … LIMIT`), `UNION` involving the driving source, reading the driving source more than
once. Below the split aggregate of class 3 the same rules apply; above it, everything runs in
the merge step on the complete aggregate and is unrestricted. In class 4, everything above the
co-partitioned join runs per partition and must be partition-local.

Broadcast tables are read as of each batch, like the static side of a stream–static join in
Spark: rows already processed are not revisited when a broadcast table changes.

`nodra plan` also builds DataFusion's physical plan, so queries DataFusion cannot execute fail
before any worker starts.

## 3. Work units and assignment

Workers are numbered `0..n` (`--worker i --workers n`; in Kubernetes the StatefulSet ordinal).
Assignment is a pure function, so workers never talk to each other.

* **Delta source:** a unit is a data file (class 1–3) or a partition (class 4).
  `unit → worker = fnv1a64(key) mod n`.
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
| Delta, incremental | `nodra/<job>/<source>/w<i>of<n>` (generation 0), `…/g<g>/w<i>of<n>` | last fully processed source table version |
| Delta, initial snapshot | `<worker key>/snapshot` | snapshot version and number of files done |
| Delta, job start | `nodra/<job>/<source>/start` | source version all first-generation workers start from |
| Stream, job start (`start = latest`) | `nodra/<job>/<source>/start/p<partition>` | high watermark of the partition when the job started |
| Stream partition | `nodra/<job>/<source>/p<partition>` | next offset to read |
| Class 4 partition | `nodra/<job>/part/<fnv(partition)>` | fingerprint of the partition's files |
| Merge | `nodra/<job>/merge` | partials table version merged |

**Commit conflicts.** Workers commit concurrently, and a commit carrying a progress key that a
concurrent commit also wrote fails delta-rs' conflict check (`ConcurrentTransaction`): the batch
is abandoned and the worker re-synchronizes. This is how a zombie replica of a worker is fenced.
The invariant behind it: every batch is computed from progress read at a snapshot *S*, and it is
committed with *S* as the read snapshot, so every commit after *S* is conflict-checked.

* A worker keeps the snapshot of its last commit (refreshed every 5 minutes) as the read snapshot
  of the next batch, with the progress it knows at that snapshot. If other commits landed
  between its read snapshot and its own commit, their `txn` actions are read from the log and
  applied to the cache, because the next batch can no longer be conflict-checked against them.
* If the target did not exist when the batch started, the table created (or opened) for the
  commit becomes the read snapshot, and the batch's progress is re-verified against it.
* Conflicts caused by concurrent deletes are retried on a fresh snapshot after re-verifying the
  batch's progress.
* Commits whose `remove` actions were computed from the read snapshot (the merge step,
  `mode => 'overwrite'`) are made without delta-rs retries, and recomputed on a newer snapshot
  instead: delta-rs would otherwise accept, for example, a concurrent OPTIMIZE that rewrote the
  files being removed, and the compacted copy of the old rows would survive.

**Delta sources.** Incremental input is read from the source's commit files
(`_delta_log/<version>.json`). `add` actions with `dataChange = false` (OPTIMIZE/compaction
rewrites) are skipped. `remove` actions with `dataChange = true` (deletes/updates) fail the batch
unless `nodra.ignore_changes = true`, matching Spark's streaming semantics. If the needed commit
files were already cleaned up, the worker stops with an error instead of guessing.

**Initial snapshot.** Without progress, a worker processes the source's snapshot in chunks of
`nodra.max_files_per_batch` files. All workers use the same snapshot version: the first worker
to commit records it (`…/start`), and workers that start later use it too — snapshots at
different versions would lose or repeat rows around a compaction committed in between. The
snapshot key records the snapshot version and the number of files done (so a resumed run
continues with the next file even if the chunk size changed) and is written by every snapshot
commit, so two replicas of a worker conflict even if they chunk differently.

**Streams.** With `nodra.start = 'latest'`, the first worker to commit records the high
watermark of every partition (`…/start/p<k>`). A partition without progress starts at its
recorded offset; a partition without a record appeared after the start and is read from its
earliest offset. Workers that start late therefore skip nothing that arrived after the job
started.

## 5. Rescaling

Stream progress is per partition, and class-4 progress per Delta partition, so their worker
count can change freely. Delta-source progress is per worker (*worker i of n processed every
file with `fnv(path) mod n = i` up to version V*), so a new worker count starts a new
**generation** (`nodra::rescale`):

1. A worker that finds progress of another worker count in the target belongs to a new
   generation. It requires final progress `done[j]` for every worker `j` of the previous
   generation (otherwise it refuses to start).
2. It starts after `min(done)` and skips every file added in version `v` whose previous owner
   `j = fnv(path) mod m` already processed it (`v ≤ done[j]`).
3. The generation's first commit rewrites the previous generation's progress keys with their
   current values. A still-running old worker's next commit therefore conflicts with it, and old
   workers stop when they see a newer generation; a new worker refuses to start while old
   progress still moves.
4. New workers commit their progress right away, so the generation is complete for the next
   rescale.

Rescaling back to an earlier worker count simply starts the next generation.

## 6. Mergeable aggregation (class 3)

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
* **Canonical layout.** Physical optimizers may order the group keys differently on workers and
  in the merge step (they see different statistics). Partial states are therefore stored with
  the group columns in the order of the *logical* aggregate, identified by name, and both sides
  map their physical layout onto it. Uncorrelated scalar subqueries are planned as joins, so the
  worker plan is a self-contained part of the full plan; `nodra plan` builds both plans.

The merge commit carries `nodra/<job>/merge = <partials version>`, so merging is idempotent.

## 7. Co-partitioned joins (class 4)

Two Delta tables partitioned by the same columns and joined with equality on all of them are
processed per partition: worker `fnv(partition values) mod n` joins the files of that partition
from both tables. Class 4 runs in batch mode (`--once`): a partition is processed once it has
files in both tables, and marked with a `txn` entry holding a fingerprint of its files. Files
added to a partition after it was processed are not picked up; the worker logs a warning when it
sees a processed partition whose files changed.

## 8. Components

| Module | Responsibility |
| --- | --- |
| `job` | parse job files, settings, sink and source calls; rewrite the query |
| `planner` | build the logical plan, pick the driving source, classify, explain |
| `delta` | Delta access: snapshot, incremental commits, create, commit with `txn` |
| `scan` | `TableProvider` over an explicit file list with partition values (partition pruning; Parquet statistics pruning through DataFusion) |
| `source` | Delta, Kafka (feature `kafka`) and `jsonl_stream` sources |
| `exec` | the worker loop: batches, progress, retries, shutdown |
| `rescale` | worker generations of Delta-driven jobs |
| `sink` | write Parquet files and build `add` actions |
| `partial` / `merge` | split aggregations and run the merge step |
| `metrics` | Prometheus text endpoint |
