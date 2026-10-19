# Operations

## Running workers

Every worker is the same binary with a different index:

```sh
nodra run job.sql --worker 2 --workers 8        # or NODRA_WORKER / NODRA_WORKERS
nodra run job.sql --worker auto --workers 8     # index = trailing number of the host name
```

`--worker auto` takes the StatefulSet ordinal from the pod name (`nodra-hourly-3` → 3), so a
StatefulSet with `replicas: 8` and `--workers 8` is all that is needed. See
[`deploy/kubernetes/statefulset.yaml`](../deploy/kubernetes/statefulset.yaml).

* **Streaming** (default): each worker loops — process available input, commit, wait
  `nodra.trigger`. `SIGTERM` finishes the current batch, a second signal aborts.
* **Batch** (`--once`): process the input available at start, then exit. Good for cron jobs and
  backfills. For class 3 jobs run `nodra merge job.sql --once` after the workers finished (with
  `--local-workers` this happens automatically).
* **Local** (`--local-workers n`): n workers in one process, e.g. for small jobs or tests.

Errors end the process with a non-zero exit code; the orchestrator restarts it and the worker
resumes from its last commit. A batch is never committed partially.

## Class 3: the merge step

Worker 0 runs the merge every `nodra.merge_interval` while streaming. Alternatively run
`nodra merge job.sql` as its own process (only one merge can commit a given partials version;
concurrent merges are harmless). The sink is consistent after every merge: it shows all partial
states committed up to the merged version.

## Azure

Build with `--features azure` and use `abfss://container@account.dfs.core.windows.net/path`
locations. Credentials come from the standard `object_store` environment variables, e.g.
`AZURE_STORAGE_ACCOUNT_NAME` plus `AZURE_STORAGE_ACCOUNT_KEY`, a service principal
(`AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`, `AZURE_TENANT_ID`) or workload identity
(`AZURE_FEDERATED_TOKEN_FILE`). Azure's conditional writes give Delta its atomic commits; no lock
service is needed.

## Kafka

Build with `--features kafka` (librdkafka is compiled from source; needs a C toolchain, `make`
and zlib headers). Offsets live in the sink's Delta log, so Kafka consumer-group offsets are
neither read nor committed; consumer lag tools will not show Nodra's progress — use
`nodra status` or the `nodra_lag` metric instead. Make sure topic retention is longer than the
longest outage you want to survive: a worker whose next offset was already deleted stops with an
error instead of skipping data.

## Monitoring

`--metrics-addr 0.0.0.0:9464` serves Prometheus metrics, labelled by `job` and `worker`:

| Metric | Meaning |
| --- | --- |
| `nodra_batches_total` | committed batches |
| `nodra_rows_in_total`, `nodra_rows_out_total` | rows read / written (class 3: partial-state rows) |
| `nodra_files_written_total` | data files written |
| `nodra_lag` | unprocessed input: records (streams) or source versions (Delta) |
| `nodra_last_batch_milliseconds` | duration of the last batch |
| `nodra_last_commit_timestamp_seconds` | time of the last commit — alert if it stops moving |
| `nodra_commit_retries_total` | commits retried after concurrent changes to the target |
| `nodra_progress_conflicts_total` | batches abandoned because another process committed the same progress (zombie replica) |
| `nodra_bad_messages_total` | skipped malformed stream messages |

## Table maintenance

Nodra writes many small commits. Run your usual maintenance on sinks and partials tables:
`OPTIMIZE` (compaction commits are safe: they are `dataChange = false`), `VACUUM` (also removes
files of batches that crashed before their commit) and log cleanup. Nodra writes checkpoints
every 100 commits and cleans up expired log files on roughly every 100th commit.

Do not delete or rewrite rows of a source table that a Nodra job reads incrementally unless the
job runs with `nodra.ignore_changes = true`.

## Rescaling

* Stream sources: progress is per partition — change `--workers` at any time.
* Delta driving sources: progress is per worker, so a different worker count is refused. To
  rescale, stop all workers once `nodra status` shows the same source version for all of them,
  and start the new set under a new `nodra.job_name` with `nodra.start = 'latest'`.

## Changing a job

Progress keys contain the job name and source locations. Changing the query keeps the progress
(new logic applies to new data only); renaming the job or a source location starts from scratch.
If the source schema changes, workers stop and must be restarted, which re-plans the job.
