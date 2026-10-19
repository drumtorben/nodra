# Job reference

A job file contains optional `SET` statements and exactly one `INSERT INTO … SELECT …`:

```sql
-- comments are allowed
SET nodra.trigger = '30s';
SET datafusion.execution.batch_size = 16384;   -- any DataFusion setting

INSERT INTO delta('/lake/out', partition_by => 'day')
SELECT … FROM delta('/lake/events') e JOIN delta('/lake/dim') d ON …;
```

The query is ordinary DataFusion SQL. Arguments of sources and sinks are literals; named
arguments use `name => value`.

## Sources

### `delta(location [, version => n] [, broadcast => true|false])`

A Delta table: a local path or a URL (`abfss://container@account.dfs.core.windows.net/path`
with the `azure` feature).

* As the **driving source**, files are split across workers. The planner picks the largest
  table that can be split (see `nodra plan`) and keeps that choice once the job has run. The first run processes the
  table's current snapshot (in chunks of `nodra.max_files_per_batch` files), later runs process
  the commits since the last run. Partition columns are exposed after the data columns.
* As a **broadcast source**, every worker loads the latest snapshot in every batch. A table is
  broadcast if it is not the driving source and its size is at most `nodra.broadcast_threshold`;
  `broadcast => true|false` overrides that.
* `version => n` pins the table to a version (the driving source then processes that snapshot
  only).

Not supported: deletion vectors, column mapping, absolute file paths (shallow clones).

### `kafka(topic, brokers => '…', schema => '…' [, format => 'json'] [, "<librdkafka.property>" => '…'])`

Requires the `kafka` cargo feature (`kafka-ssl` for TLS and SASL, e.g. Azure Event Hubs).
Partitions are assigned to workers with `partition mod workers`; offsets are stored in the
sink's Delta log, not in Kafka. `schema` declares the
columns read from each JSON message, e.g. `'ts TIMESTAMP, campaign_id BIGINT, user_id VARCHAR'`.
Unknown fields are ignored, missing ones are NULL. Any argument whose name contains a dot is
passed to librdkafka, e.g. `"security.protocol" => 'SASL_SSL'`.

### `jsonl_stream(directory, schema => '…')`

A directory with one file per partition (`0.jsonl`, `1.jsonl`, …) and one JSON message per line;
the line number is the offset. Behaves like a Kafka topic without a broker (tests, demos,
replaying captured data).

## Sink

### `delta(location [, partition_by => 'col1, col2'] [, mode => 'append'|'overwrite'])`

The table is created on the first commit with the query's output schema. Types Delta cannot
store are converted (`UINT64` → `BIGINT`, nanosecond timestamps → microsecond `TIMESTAMP` in
UTC, string views → `STRING`); `nodra plan` shows the stored types. Output column names must be
valid Delta names, so give expressions an alias (`count(*) AS n`).

* `append` (default): every batch adds its rows (classes 1, 2, 4) or the merge step replaces the
  changed partitions (class 3).
* `overwrite`: recompute the whole table from the complete source in one batch. Classes 1/2 over
  Delta sources only, one worker, `--once`.

For class 3 jobs, partitioning the sink by a `GROUP BY` key (typically a day) makes merges
*partition-scoped*: only partitions that received new data are recomputed.

## Settings

| Key | Default | Meaning |
| --- | --- | --- |
| `nodra.job_name` | file stem | Name used in progress keys; changing it restarts the job from scratch. |
| `nodra.trigger` | `60s` | Pause between micro-batches in streaming mode. |
| `nodra.broadcast_threshold` | `1GiB` | Delta tables up to this size are broadcast to every worker. |
| `nodra.max_files_per_batch` | `1000` | Maximum number of new Delta files a worker reads per batch. |
| `nodra.max_records_per_partition` | `100000` | Maximum stream records read per partition per batch. |
| `nodra.ignore_changes` | `false` | Skip deletes/updates in Delta sources instead of failing. |
| `nodra.start` | `earliest` | Where a source starts without progress: `earliest` or `latest`. |
| `nodra.merge_interval` | `60s` | How often worker 0 runs the merge step of a class-3 job (`0` = after every batch). |
| `nodra.partials_location` | `<sink>__nodra_partials` | Delta table that stores partial aggregate states. |
| `nodra.commit_retries` | `50` | How often a commit that lost a log-version race is retried. |
| `nodra.bad_messages` | `fail` | Stream messages that do not match the schema: `fail` the job or `skip` them (logged, counted in `nodra_bad_messages_total`). |

Durations accept `ms`, `s`, `m`, `h`; sizes accept `KB/MB/GB/TB` (binary multiples) and the
`KiB` forms. Unknown `nodra.*` keys are an error. Settings can be overridden on the command line
with `--set key=value`.

## Progress keys

Progress is stored as Delta `txn` actions (`appId` → version) in the table the worker commits
to (the sink, or the partials table for class 3). `nodra status` prints them. `<source>` is the
source's canonical location (`delta:/lake/events`, `kafka:clicks`): `/lake/events`,
`/lake/./events/` and `file:///lake/events` are the same source.

| Key | Version |
| --- | --- |
| `nodra/<job>/<source>/w<i>of<n>` | last processed source version (Delta driving source) |
| `nodra/<job>/<source>/g<g>/w<i>of<n>` | the same, after the worker count changed (generation `g`) |
| `<worker key>/snapshot` | snapshot version and files done during the first run |
| `nodra/<job>/<source>/start` | source version all first workers start from (Delta driving source) |
| `nodra/<job>/<source>/p<partition>` | next offset to read (stream) |
| `nodra/<job>/<source>/start/p<partition>` | high watermark at the job start (stream, `start = latest`) |
| `nodra/<job>/part/<hash>` | fingerprint of a processed partition's files (class 4) |
| `nodra/<job>/merge` | partials-table version merged (class 3, in the sink) |
