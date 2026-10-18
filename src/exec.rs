//! Worker execution: the batch loop, progress handling and commits.
//!
//! See `docs/design.md`, sections 3–4. In short, every batch
//!
//! 1. loads the *target* table (the sink, or the partial-state table for class 3) and reads the
//!    worker's progress from its `txn` actions,
//! 2. determines the input after that progress,
//! 3. runs the query with DataFusion and writes Parquet files into the target directory,
//! 4. commits the files and the new progress in one Delta commit on top of the snapshot read in
//!    step 1. A concurrent commit of the same progress key fails the commit (delta-rs conflict
//!    check), the batch is abandoned and re-planned.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use datafusion::arrow::compute::concat_batches;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::MemTable;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::physical_plan::execute_stream;
use datafusion::prelude::SessionContext;
use deltalake_core::kernel::{Action, Transaction};
use deltalake_core::DeltaTable;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::delta::{self, CommitOutcome, DataFile};
use crate::error::{NodraError, Result};
use crate::hash::{fnv1a64, worker_for};
use crate::job::{source_table, Job, SinkMode, SourceSpec};
use crate::metrics::{self, WorkerMetrics};
use crate::partial;
use crate::planner::{self, JobClass, JobPlan, Role, SourceInfo, UnitKey};
use crate::scan::FileListTable;
use crate::session;
use crate::settings::StartPosition;
use crate::sink;
use crate::source::jsonl::JsonlStream;
use crate::source::stream::{decode_json, decode_json_lenient, StreamRef};
use crate::types;

/// Location of the partial-state table of a class-3 job.
pub fn partials_location(job: &Job) -> String {
    job.settings
        .get("nodra.partials_location")
        .map(str::to_string)
        .unwrap_or_else(|| format!("{}__nodra_partials", job.sink.uri().trim_end_matches('/')))
}

/// A planned job, ready to run.
#[derive(Debug)]
pub struct Pipeline {
    /// The job.
    pub job: Job,
    /// Planner output.
    pub plan: JobPlan,
    /// Source information used for planning.
    pub infos: Vec<SourceInfo>,
    /// Schema written to the sink (Delta-compatible types).
    pub sink_schema: SchemaRef,
    /// Physical layout of each Delta source (as planned).
    pub layouts: Vec<Option<delta::Layout>>,
}

/// Collect what the planner needs to know about each source, and the layouts of Delta sources.
pub async fn source_infos(job: &Job) -> Result<(Vec<SourceInfo>, Vec<Option<delta::Layout>>)> {
    let mut infos = Vec::with_capacity(job.sources.len());
    let mut layouts = Vec::with_capacity(job.sources.len());
    for spec in &job.sources {
        match spec {
            SourceSpec::Delta { uri, version, .. } => {
                let t = match version {
                    Some(v) => delta::open_version(uri, *v).await?,
                    None => delta::open(uri).await?,
                };
                let files = delta::active_files(&t)?;
                infos.push(SourceInfo {
                    schema: delta::exposed_schema(&t)?,
                    partition_columns: delta::partition_columns(&t)?,
                    size_bytes: Some(files.iter().map(|f| f.size).sum()),
                    num_files: Some(files.len()),
                    version: Some(delta::version(&t)? as i64),
                });
                layouts.push(Some(delta::Layout::of(&t)?));
            }
            SourceSpec::Kafka { schema, .. } | SourceSpec::JsonlStream { schema, .. } => {
                infos.push(SourceInfo {
                    schema: types::parse_columns(schema).await?,
                    partition_columns: Vec::new(),
                    size_bytes: None,
                    num_files: None,
                    version: None,
                });
                layouts.push(None);
            }
        }
    }
    Ok((infos, layouts))
}

impl Pipeline {
    /// Inspect the sources and plan the job.
    pub async fn prepare(job: Job) -> Result<Pipeline> {
        let (infos, layouts) = source_infos(&job).await?;
        let plan = planner::plan_job(&job, &infos).await?;
        let sink_schema = types::delta_schema(&plan.output_schema)?;
        if matches!(job.sink, crate::job::SinkSpec::Delta { mode: SinkMode::Overwrite, .. })
            && (job.stream_source().is_some() || plan.class != JobClass::PartitionParallel && plan.class != JobClass::BroadcastJoin)
        {
            return Err(NodraError::job(
                "mode => 'overwrite' is only supported for class-1/2 jobs over Delta sources",
            ));
        }
        for p in job.sink.partition_by() {
            if sink_schema.field_with_name(p).is_err() {
                return Err(NodraError::job(format!(
                    "partition column `{p}` is not an output column of the query"
                )));
            }
        }
        Ok(Pipeline { job, plan, infos, sink_schema, layouts })
    }

    /// True for `mode => 'overwrite'` sinks.
    pub fn overwrite(&self) -> bool {
        matches!(self.job.sink, crate::job::SinkSpec::Delta { mode: SinkMode::Overwrite, .. })
    }

    /// `nodra plan --explain` text.
    pub fn explain(&self) -> String {
        planner::explain(&self.job, &self.infos, &self.plan)
    }

    fn prefix(&self) -> String {
        format!("nodra/{}", self.job.name)
    }

    /// Progress key of a Delta driving source for worker `i` of `n`.
    pub fn delta_progress_key(&self, source: usize, i: u32, n: u32) -> String {
        format!("{}/{}/w{i}of{n}", self.prefix(), self.job.sources[source].key())
    }

    /// Progress key of a stream partition.
    pub fn stream_progress_key(&self, source: usize, partition: i32) -> String {
        format!("{}/{}/p{partition}", self.prefix(), self.job.sources[source].key())
    }

    /// Progress key of a class-4 partition.
    pub fn partition_progress_key(&self, partition_key: &str) -> String {
        format!("{}/part/{:016x}", self.prefix(), fnv1a64(partition_key.as_bytes()))
    }

    /// Progress key of the merge step.
    pub fn merge_progress_key(&self) -> String {
        format!("{}/merge", self.prefix())
    }

    /// Location of the table that receives worker commits.
    pub fn target_location(&self) -> String {
        match self.plan.class {
            JobClass::MergeableAggregation => partials_location(&self.job),
            _ => self.job.sink.uri().to_string(),
        }
    }
}

/// Which worker this is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerId {
    /// 0-based index.
    pub index: u32,
    /// Total number of workers.
    pub count: u32,
}

/// Run the workers `indices` (of `count`) in this process. With `once`, each worker processes
/// the input available at start and returns; otherwise workers run until `stop` turns true.
pub async fn run(
    p: Arc<Pipeline>,
    indices: Vec<u32>,
    count: u32,
    once: bool,
    stop: watch::Receiver<bool>,
) -> Result<()> {
    if count == 0 || indices.iter().any(|i| *i >= count) {
        return Err(NodraError::config(format!("invalid worker indices {indices:?} for {count} workers")));
    }
    if p.overwrite() && (count != 1 || !once) {
        return Err(NodraError::config("mode => 'overwrite' requires a single worker and --once"));
    }
    if p.plan.class == JobClass::CoPartitionedJoin && !once {
        warn!("class-4 jobs process each partition once; running continuously picks up new partitions only");
    }
    info!(job = %p.job.name, class = p.plan.class.number(), workers = count, local = ?indices, once, "starting");
    let mut handles = Vec::new();
    let (fail_tx, fail_rx) = watch::channel(false);
    for i in indices.iter().copied() {
        let p = p.clone();
        let stop = stop.clone();
        let mut fail_rx = fail_rx.clone();
        let fail_tx = fail_tx.clone();
        handles.push(tokio::spawn(async move {
            let id = WorkerId { index: i, count };
            // If a sibling worker fails, stop this one too (the process exits with the error).
            let res = tokio::select! {
                r = run_worker(p, id, once, stop) => r,
                _ = wait_true(&mut fail_rx) => Ok(()),
            };
            if res.is_err() {
                let _ = fail_tx.send(true);
            }
            res
        }));
    }
    let mut first_err = None;
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => {
                first_err.get_or_insert(e);
            }
            Err(e) => {
                first_err.get_or_insert(NodraError::job(format!("worker task failed: {e}")));
            }
        }
    }
    if let Some(e) = first_err {
        return Err(e);
    }
    // With --once and all workers in this process, run a last merge after they are done so the
    // sink reflects all of them. Distributed --once runs merge with `nodra merge --once`.
    if once && p.plan.class == JobClass::MergeableAggregation && indices.len() as u32 == count {
        crate::merge::merge(&p).await?;
    }
    Ok(())
}

async fn wait_true(rx: &mut watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            // Sender gone: never fires.
            std::future::pending::<()>().await;
        }
    }
}

/// Outcome of one batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Step {
    /// Input was processed; `more` is true if more input is already available.
    Processed { more: bool },
    /// Nothing to do.
    Idle,
    /// Another process committed the same progress first.
    Conflict,
}

/// Run one worker until `stop` (or, with `once`, until it is caught up).
pub async fn run_worker(p: Arc<Pipeline>, id: WorkerId, once: bool, mut stop: watch::Receiver<bool>) -> Result<()> {
    let mut w = Worker::new(p.clone(), id).await?;
    let trigger = p.job.settings.trigger()?;
    let merge_interval = p.job.settings.merge_interval()?;
    let mut last_merge = Instant::now();
    loop {
        if *stop.borrow() {
            break;
        }
        let started = Instant::now();
        let step = w.step().await?;
        w.metrics.last_batch_ms.store(started.elapsed().as_millis() as u64, Ordering::Relaxed);
        debug!(worker = id.index, ?step, "batch done");
        if !once
            && id.index == 0
            && p.plan.class == JobClass::MergeableAggregation
            && last_merge.elapsed() >= merge_interval
        {
            crate::merge::merge(&p).await?;
            last_merge = Instant::now();
        }
        match step {
            Step::Processed { more: true } | Step::Conflict => continue,
            Step::Processed { more: false } | Step::Idle if once => break,
            _ => {
                tokio::select! {
                    _ = tokio::time::sleep(trigger) => {}
                    _ = stop.changed() => {}
                }
            }
        }
    }
    info!(worker = id.index, "worker stopped");
    Ok(())
}

/// A progress update committed with a batch, and the value the worker read before the batch
/// (used to re-verify ownership when a commit has to be retried on a fresh snapshot).
#[derive(Debug, Clone)]
struct Progress {
    txn: Transaction,
    expected: Option<i64>,
}

impl Progress {
    fn new(key: impl ToString, version: i64, expected: Option<i64>) -> Self {
        Self { txn: Transaction::new(key, version), expected }
    }
}

/// Input selected for one batch.
enum Input {
    /// Files of Delta driving source(s), by source index.
    Files(Vec<(usize, DeltaTable, Vec<DataFile>)>),
    /// A decoded stream batch.
    Stream(datafusion::arrow::array::RecordBatch),
}

/// How long a cached target snapshot is used as the read snapshot of the next commit before it
/// is reloaded (bounds the number of concurrent commits a commit has to check for conflicts).
const SNAPSHOT_TTL: Duration = Duration::from_secs(300);

struct Worker {
    p: Arc<Pipeline>,
    id: WorkerId,
    metrics: Arc<WorkerMetrics>,
    stream: Option<(usize, StreamRef, SchemaRef)>,
    /// Delta progress known to be safe to skip to (versions without files for this worker).
    cursor: Option<i64>,
    /// Partials table schema (class 3), known after the first plan.
    state_schema: Option<SchemaRef>,
    /// Target snapshot used as the read snapshot of the next commit. After a successful commit
    /// it is the post-commit snapshot, so the next commit's conflict check covers every commit
    /// made by others since then (including a zombie replica committing our progress keys).
    target: Option<DeltaTable>,
    /// Progress values valid at `target`.
    known: HashMap<String, Option<i64>>,
    /// All `txn` versions of `target` when it was loaded (None if not available).
    all_txns: Option<HashMap<String, i64>>,
    loaded_at: Option<Instant>,
}

impl Worker {
    async fn new(p: Arc<Pipeline>, id: WorkerId) -> Result<Self> {
        let metrics = metrics::worker(&p.job.name, id.index);
        let stream = match p.job.stream_source() {
            None => None,
            Some(i) => {
                let schema = p.infos[i].schema.clone();
                let src: StreamRef = match &p.job.sources[i] {
                    SourceSpec::JsonlStream { dir, .. } => Arc::new(JsonlStream::new(dir)?),
                    #[cfg(feature = "kafka")]
                    SourceSpec::Kafka { topic, brokers, properties, .. } => Arc::new(
                        crate::source::kafka::KafkaSource::new(topic, brokers, &format!("nodra-{}", p.job.name), properties)?,
                    ),
                    #[cfg(not(feature = "kafka"))]
                    SourceSpec::Kafka { .. } => {
                        return Err(NodraError::config("this build of nodra has no Kafka support (feature `kafka`)"))
                    }
                    SourceSpec::Delta { .. } => unreachable!(),
                };
                Some((i, src, schema))
            }
        };
        Ok(Self {
            p,
            id,
            metrics,
            stream,
            cursor: None,
            state_schema: None,
            target: None,
            known: HashMap::new(),
            all_txns: None,
            loaded_at: None,
        })
    }

    async fn step(&mut self) -> Result<Step> {
        let target = self.target_snapshot().await?;
        let res = if self.p.plan.class == JobClass::CoPartitionedJoin {
            self.step_copartitioned(target).await
        } else if self.stream.is_some() {
            self.step_stream(target).await
        } else {
            self.step_delta(target).await
        };
        if !matches!(res, Ok(Step::Processed { .. }) | Ok(Step::Idle)) {
            self.invalidate();
        }
        res
    }

    /// The target snapshot for this batch: the cached one, or a freshly loaded one. The target
    /// may not exist before the first batch; it is created when the first results are written
    /// (for class 3 its schema is only known then).
    async fn target_snapshot(&mut self) -> Result<Option<DeltaTable>> {
        let fresh = self.loaded_at.map(|t| t.elapsed() < SNAPSHOT_TTL).unwrap_or(false);
        if let (Some(t), true) = (&self.target, fresh) {
            return Ok(Some(t.clone()));
        }
        self.invalidate();
        let t = delta::try_open(&self.p.target_location()).await?;
        if let Some(t) = &t {
            self.all_txns = delta::txn_map(t).await?;
        }
        self.target = t.clone();
        self.loaded_at = Some(Instant::now());
        Ok(t)
    }

    fn invalidate(&mut self) {
        self.target = None;
        self.known.clear();
        self.all_txns = None;
        self.loaded_at = None;
    }

    /// Committed progress for `key` as of the batch's target snapshot.
    async fn progress(&mut self, target: &Option<DeltaTable>, key: &str) -> Result<Option<i64>> {
        if let Some(v) = self.known.get(key) {
            return Ok(*v);
        }
        let v = match (target, &self.all_txns) {
            (None, _) => None,
            (Some(_), Some(all)) => all.get(key).copied(),
            (Some(t), None) => delta::txn_version(t, key).await?,
        };
        self.known.insert(key.to_string(), v);
        Ok(v)
    }

    /// Remember a successful commit: its snapshot becomes the next read snapshot.
    fn committed(&mut self, table: DeltaTable, progress: &[Progress]) {
        for pr in progress {
            self.known.insert(pr.txn.app_id.clone(), Some(pr.txn.version));
        }
        self.target = Some(table);
        if self.loaded_at.is_none() {
            self.loaded_at = Some(Instant::now());
        }
    }

    // ---------------------------------------------------------------- Delta driving source --

    async fn step_delta(&mut self, target: Option<DeltaTable>) -> Result<Step> {
        let p = self.p.clone();
        let di = p.plan.driving()[0];
        let SourceSpec::Delta { uri, version: pinned, .. } = &p.job.sources[di] else { unreachable!() };
        if p.overwrite() {
            return self.step_overwrite(target, di, uri, *pinned).await;
        }
        let a_key = p.delta_progress_key(di, self.id.index, self.id.count);
        let b_key = format!("{a_key}/snapshot");
        let committed = self.progress(&target, &a_key).await?;
        if committed.is_none() {
            self.check_rescale(&target, di).await?;
        }
        let progress = match (committed, self.cursor) {
            (Some(c), Some(k)) if k > c => Some(k),
            (c, _) => c,
        };
        // Incremental batches only need the commit files, not a loaded snapshot (which would
        // replay the whole log of a possibly huge table).
        let src = delta::handle(uri)?;
        let latest = match pinned {
            Some(v) => *v,
            None => delta::latest_version(&src, progress.unwrap_or(0).max(0) as u64).await? as i64,
        };
        let max_files = p.job.settings.max_files_per_batch()?;
        let layout = p.layouts[di].clone().expect("delta layout");
        match progress {
            None => {
                if p.job.settings.start()? == StartPosition::Latest && pinned.is_none() {
                    info!(worker = self.id.index, version = latest, "starting at the latest source version");
                    return self.commit_progress_only(target, vec![Progress::new(&a_key, latest, None)]).await;
                }
                // Initial snapshot, processed in chunks of `max_files` files.
                let b_committed = self.progress(&target, &b_key).await?;
                let (snap_version, done) = match b_committed {
                    Some(b) => (b >> 24, (b & 0xff_ffff) as usize),
                    None => (latest, 0),
                };
                let snap = delta::open_version(uri, snap_version).await?;
                self.check_source_schema(di, &snap)?;
                let mut files: Vec<DataFile> = delta::active_files(&snap)?
                    .into_iter()
                    .filter(|f| self.owns_file(f))
                    .collect();
                files.sort_by(|a, b| a.path.as_ref().cmp(b.path.as_ref()));
                let total_chunks = files.len().div_ceil(max_files).max(1);
                let chunk: Vec<DataFile> = files.into_iter().skip(done * max_files).take(max_files).collect();
                let last = done + 1 >= total_chunks;
                let txn = if last {
                    Progress::new(&a_key, snap_version, None)
                } else {
                    Progress::new(&b_key, (snap_version << 24) | (done as i64 + 1), b_committed)
                };
                self.metrics.lag.store((latest - snap_version) as u64, Ordering::Relaxed);
                let step = self.process(target, Input::Files(vec![(di, snap, chunk)]), vec![txn]).await?;
                Ok(match step {
                    Step::Processed { .. } => Step::Processed { more: !last || snap_version < latest },
                    other => other,
                })
            }
            Some(prog) if prog < latest => {
                let end = latest.min(prog + 1000);
                let changes = delta::changes(&src, &layout.partition_columns(), prog as u64, end as u64).await?;
                let ignore_changes = p.job.settings.ignore_changes()?;
                let mut files = Vec::new();
                let mut v_end = prog;
                for c in changes {
                    if c.metadata_changed || c.protocol_changed {
                        // Schema or protocol change: compare with the planned schema (a restart
                        // re-plans; until then the new data could not be read correctly).
                        let at = delta::open_version(uri, c.version as i64).await?;
                        self.check_source_schema(di, &at)?;
                    }
                    if c.data_removes > 0 && !ignore_changes {
                        return Err(NodraError::delta(format!(
                            "version {} of {uri} deletes or updates data; Nodra only processes appends \
                             (set nodra.ignore_changes = true to skip such commits)",
                            c.version
                        )));
                    }
                    files.extend(c.adds.into_iter().filter(|f| self.owns_file(f)));
                    v_end = c.version as i64;
                    if files.len() >= max_files {
                        break;
                    }
                }
                self.metrics.lag.store((latest - v_end) as u64, Ordering::Relaxed);
                let more = v_end < latest;
                if files.is_empty() {
                    // Nothing for this worker in these versions: remember, commit rarely.
                    self.cursor = Some(v_end);
                    if v_end - committed.unwrap_or(-1) >= 100 {
                        return self.commit_progress_only(target, vec![Progress::new(&a_key, v_end, committed)]).await;
                    }
                    return Ok(if more { Step::Processed { more } } else { Step::Idle });
                }
                let step = self
                    .process(target, Input::Files(vec![(di, src, files)]), vec![Progress::new(&a_key, v_end, committed)])
                    .await?;
                if step != Step::Conflict {
                    self.cursor = Some(v_end);
                }
                Ok(match step {
                    Step::Processed { .. } => Step::Processed { more },
                    other => other,
                })
            }
            Some(_) => {
                self.metrics.lag.store(0, Ordering::Relaxed);
                Ok(Step::Idle)
            }
        }
    }

    /// `mode => 'overwrite'`: recompute the sink from the complete current source snapshot in
    /// one batch (no progress tracking; single worker, `--once`).
    async fn step_overwrite(&mut self, target: Option<DeltaTable>, di: usize, uri: &str, pinned: Option<i64>) -> Result<Step> {
        if self.cursor.is_some() {
            return Ok(Step::Idle);
        }
        let src = match pinned {
            Some(v) => delta::open_version(uri, v).await?,
            None => delta::open(uri).await?,
        };
        self.check_source_schema(di, &src)?;
        let version = delta::version(&src)? as i64;
        let files = delta::active_files(&src)?;
        let step = self.process(target, Input::Files(vec![(di, src, files)]), Vec::new()).await?;
        self.cursor = Some(version);
        Ok(step)
    }

    fn owns_file(&self, f: &DataFile) -> bool {
        let key = match &self.p.plan.unit_key {
            UnitKey::Partition(_) => f.partition_key(),
            _ => f.path.to_string(),
        };
        worker_for(&key, self.id.count) == self.id.index
    }

    /// Refuse to start a Delta-driven job with a new worker count while progress of another
    /// worker count exists (files would be assigned differently).
    async fn check_rescale(&self, target: &Option<DeltaTable>, di: usize) -> Result<()> {
        let Some(t) = target else { return Ok(()) };
        let other = |n: u32| {
            NodraError::config(format!(
                "the job was run with {n} workers before; changing the worker count of a \
                 Delta-driven job is not supported yet (see docs/design.md, Rescaling)"
            ))
        };
        match &self.all_txns {
            Some(all) => {
                // Keys look like `<prefix>/w<i>of<n>` (and `<prefix>/w<i>of<n>/snapshot`).
                let prefix = self.p.delta_progress_key(di, 0, 1);
                let prefix = prefix.trim_end_matches("w0of1");
                for k in all.keys() {
                    if let Some(rest) = k.strip_prefix(prefix) {
                        let n = rest.split('/').next().and_then(|w| w.split_once("of")).and_then(|(_, n)| n.parse::<u32>().ok());
                        if let Some(n) = n {
                            if n != self.id.count {
                                return Err(other(n));
                            }
                        }
                    }
                }
            }
            None => {
                for n in (1..=64u32).filter(|n| *n != self.id.count) {
                    if delta::txn_version(t, &self.p.delta_progress_key(di, 0, n)).await?.is_some() {
                        return Err(other(n));
                    }
                }
            }
        }
        Ok(())
    }

    fn check_source_schema(&self, di: usize, t: &DeltaTable) -> Result<()> {
        let now = delta::exposed_schema(t)?;
        let planned = &self.p.infos[di].schema;
        if now.fields() != planned.fields() {
            return Err(NodraError::delta(format!(
                "the schema of {} changed since the job was planned; restart the job",
                self.p.job.sources[di].describe()
            )));
        }
        Ok(())
    }

    // ---------------------------------------------------------------- stream source --------

    async fn step_stream(&mut self, target: Option<DeltaTable>) -> Result<Step> {
        let (si, src, schema) = self.stream.clone().expect("stream source");
        let start = self.p.job.settings.start()?;
        let max = self.p.job.settings.max_records_per_partition()?;
        let skip_bad = self.p.job.settings.skip_bad_messages()?;
        let parts: Vec<i32> = src
            .partitions()
            .await?
            .into_iter()
            .filter(|p| (p.rem_euclid(self.id.count as i32)) as u32 == self.id.index)
            .collect();
        if parts.is_empty() {
            return Ok(Step::Idle);
        }
        let mut batches = Vec::new();
        let mut txns = Vec::new();
        let mut more = false;
        let mut lag = 0i64;
        for part in parts {
            let key = self.p.stream_progress_key(si, part);
            let committed = self.progress(&target, &key).await?;
            let (low, high) = src.watermarks(part).await?;
            let from = match (committed, start) {
                (Some(o), _) => o,
                (None, StartPosition::Earliest) => low,
                (None, StartPosition::Latest) => {
                    txns.push(Progress::new(&key, high, None));
                    high
                }
            };
            if from < low {
                return Err(NodraError::Source(format!(
                    "{}: partition {part} no longer has offset {from} (earliest available: {low}); \
                     data was deleted before it was processed",
                    self.p.job.sources[si].describe()
                )));
            }
            if from >= high {
                continue;
            }
            let fetched = src.fetch(part, from, high, max).await?;
            if fetched.next_offset > from {
                let ctx = format!("{} partition {part} offsets {from}..{}", self.p.job.sources[si].describe(), fetched.next_offset);
                if skip_bad {
                    let (batch, bad) = decode_json_lenient(&schema, &fetched.payloads)?;
                    for (i, e) in bad.iter().take(3) {
                        warn!(partition = part, "skipping message #{i} of {ctx}: {e}");
                    }
                    self.metrics.bad_messages.fetch_add(bad.len() as u64, Ordering::Relaxed);
                    batches.push(batch);
                } else {
                    batches.push(decode_json(&schema, &fetched.payloads, &ctx)?);
                }
                txns.retain(|t: &Progress| t.txn.app_id != key);
                txns.push(Progress::new(&key, fetched.next_offset, committed));
            }
            more |= fetched.next_offset < high;
            lag += high - fetched.next_offset;
        }
        self.metrics.lag.store(lag.max(0) as u64, Ordering::Relaxed);
        if txns.is_empty() {
            return Ok(Step::Idle);
        }
        let batch = concat_batches(&schema, &batches)?;
        self.metrics.rows_in.fetch_add(batch.num_rows() as u64, Ordering::Relaxed);
        let step = self.process(target, Input::Stream(batch), txns).await?;
        Ok(match step {
            Step::Processed { .. } => Step::Processed { more },
            other => other,
        })
    }

    // ---------------------------------------------------------------- class 4 --------------

    async fn step_copartitioned(&mut self, target: Option<DeltaTable>) -> Result<Step> {
        let p = self.p.clone();
        let driving = p.plan.driving();
        let max_files = p.job.settings.max_files_per_batch()?;
        let mut tables = Vec::new();
        let mut by_partition: BTreeMap<String, Vec<Vec<DataFile>>> = BTreeMap::new();
        for (slot, &di) in driving.iter().enumerate() {
            let SourceSpec::Delta { uri, version, .. } = &p.job.sources[di] else { unreachable!() };
            let t = match version {
                Some(v) => delta::open_version(uri, *v).await?,
                None => delta::open(uri).await?,
            };
            self.check_source_schema(di, &t)?;
            for f in delta::active_files(&t)? {
                let entry = by_partition.entry(f.partition_key()).or_insert_with(|| vec![Vec::new(); driving.len()]);
                entry[slot].push(f);
            }
            tables.push(t);
        }
        let mut chosen: Vec<(String, Vec<Vec<DataFile>>)> = Vec::new();
        let mut nfiles = 0;
        let mut pending = 0usize;
        for (key, files) in by_partition {
            if worker_for(&key, self.id.count) != self.id.index {
                continue;
            }
            if self.progress(&target, &p.partition_progress_key(&key)).await?.is_some() {
                continue;
            }
            pending += 1;
            if nfiles < max_files {
                nfiles += files.iter().map(Vec::len).sum::<usize>();
                chosen.push((key, files));
            }
        }
        self.metrics.lag.store(pending.saturating_sub(chosen.len()) as u64, Ordering::Relaxed);
        if chosen.is_empty() {
            return Ok(Step::Idle);
        }
        let more = pending > chosen.len();
        let txns = chosen.iter().map(|(k, _)| Progress::new(p.partition_progress_key(k), 1, None)).collect();
        let mut inputs = Vec::new();
        for (slot, (&di, t)) in driving.iter().zip(tables).enumerate() {
            let files = chosen.iter().flat_map(|(_, f)| f[slot].clone()).collect();
            inputs.push((di, t, files));
        }
        let step = self.process(target, Input::Files(inputs), txns).await?;
        Ok(match step {
            Step::Processed { .. } => Step::Processed { more },
            other => other,
        })
    }

    // ---------------------------------------------------------------- shared ---------------

    async fn commit_progress_only(&mut self, target: Option<DeltaTable>, progress: Vec<Progress>) -> Result<Step> {
        let target = match target {
            Some(t) => t,
            None => self.create_target(None).await?,
        };
        let parts = delta::partition_columns(&target)?;
        match self.commit_checked(target, Vec::new(), &progress, delta::append_op(&parts)).await? {
            Some(v) => {
                debug!(worker = self.id.index, version = v, "progress committed");
                Ok(Step::Processed { more: false })
            }
            None => {
                self.metrics.progress_conflicts.fetch_add(1, Ordering::Relaxed);
                Ok(Step::Conflict)
            }
        }
    }

    /// Commit with progress. Returns the new version, or `None` if another process committed
    /// progress for one of our keys. Conflicts caused by concurrent deletes on the target are
    /// retried on a fresh snapshot, after verifying that our progress keys are unchanged.
    async fn commit_checked(
        &mut self,
        mut target: DeltaTable,
        actions: Vec<Action>,
        progress: &[Progress],
        op: deltalake_core::protocol::DeltaOperation,
    ) -> Result<Option<u64>> {
        let retries = self.p.job.settings.commit_retries()?;
        let txns: Vec<Transaction> = progress.iter().map(|p| p.txn.clone()).collect();
        for attempt in 0..5 {
            match delta::commit(&mut target, actions.clone(), txns.clone(), op.clone(), retries).await? {
                CommitOutcome::Committed(v) => {
                    self.committed(target, progress);
                    return Ok(Some(v));
                }
                CommitOutcome::Conflict => {
                    self.invalidate();
                    return Ok(None);
                }
                CommitOutcome::Retry(why) => {
                    warn!(worker = self.id.index, attempt, "commit conflicts with a concurrent change ({why}); retrying");
                    self.metrics.commit_retries.fetch_add(1, Ordering::Relaxed);
                    target = delta::open(&self.p.target_location()).await?;
                    for pr in progress {
                        if delta::txn_version(&target, &pr.txn.app_id).await? != pr.expected {
                            return Ok(None);
                        }
                    }
                }
            }
        }
        Err(NodraError::delta("commit kept conflicting with concurrent changes to the target table"))
    }

    /// Create the target table. For class 3 the partial-state schema must be known.
    async fn create_target(&mut self, state_schema: Option<SchemaRef>) -> Result<DeltaTable> {
        let p = self.p.clone();
        match p.plan.class {
            JobClass::MergeableAggregation => {
                let schema = match state_schema.or_else(|| self.state_schema.clone()) {
                    Some(s) => s,
                    None => self.discover_state_schema().await?,
                };
                let parts = partials_partition_columns(&p);
                delta::create_or_open(&p.target_location(), &schema, &parts).await
            }
            _ => delta::create_or_open(p.job.sink.uri(), &p.sink_schema, p.job.sink.partition_by()).await,
        }
    }

    /// Plan the query once over empty inputs to learn the partial-state schema.
    async fn discover_state_schema(&mut self) -> Result<SchemaRef> {
        let ctx = session::new_context(&self.p.job.settings)?;
        register_placeholders(&ctx, &self.p).await?;
        let physical = ctx.sql(&self.p.job.query).await?.create_physical_plan().await?;
        let partial = partial::worker_plan(&physical)?;
        let schema = types::delta_schema(&partial::state_schema(&partial.schema()))?;
        self.state_schema = Some(schema.clone());
        Ok(schema)
    }

    async fn process(&mut self, target: Option<DeltaTable>, input: Input, progress: Vec<Progress>) -> Result<Step> {
        let p = self.p.clone();
        let ctx = session::new_context(&p.job.settings)?;
        let mut rows_in = 0u64;
        match input {
            Input::Files(list) => {
                let driving: BTreeSet<usize> = list.iter().map(|(i, _, _)| *i).collect();
                for (i, t, files) in list {
                    rows_in += files.iter().filter_map(|f| f.num_records).sum::<u64>();
                    register_delta(&ctx, i, &t, p.layouts[i].as_ref(), &files, true)?;
                }
                register_broadcast(&ctx, &p, &driving).await?;
            }
            Input::Stream(batch) => {
                let (si, _, schema) = self.stream.clone().expect("stream");
                let marked = partial::mark_driving(&schema);
                let batch = batch.with_schema(marked.clone())?;
                ctx.register_table(source_table(si), Arc::new(MemTable::try_new(marked, vec![vec![batch]])?))?;
                register_broadcast(&ctx, &p, &BTreeSet::from([si])).await?;
            }
        }
        let physical = ctx.sql(&p.job.query).await?.create_physical_plan().await?;
        let (plan, target_schema) = if p.plan.class == JobClass::MergeableAggregation {
            let partial = partial::worker_plan(&physical)?;
            let schema = types::delta_schema(&partial::state_schema(&partial.schema()))?;
            self.state_schema = Some(schema.clone());
            (partial, schema)
        } else {
            (physical, p.sink_schema.clone())
        };
        let target = match target {
            Some(t) => t,
            None => self.create_target(Some(target_schema.clone())).await?,
        };
        let target_schema = delta::arrow_schema(&target)?;
        let stream = execute_stream(plan, ctx.task_ctx())?;
        let written = sink::write_stream(&target, &target_schema, stream).await?;
        let nfiles = written.adds.len();
        let rows_out = written.rows;
        let mut actions: Vec<Action> = written.adds.into_iter().map(Action::Add).collect();
        let parts = delta::partition_columns(&target)?;
        let op = match p.job.sink {
            crate::job::SinkSpec::Delta { mode: SinkMode::Overwrite, .. } if p.plan.class != JobClass::MergeableAggregation => {
                actions.extend(delta::removes_for(&target, |_| false)?);
                delta::overwrite_op(&parts, None)
            }
            _ => delta::append_op(&parts),
        };
        if testing::should_fail(&p.job.name) {
            return Err(NodraError::job("injected failure before commit (test failpoint)"));
        }
        match self.commit_checked(target, actions, &progress, op).await? {
            Some(v) => {
                self.metrics.batches.fetch_add(1, Ordering::Relaxed);
                self.metrics.rows_in.fetch_add(rows_in, Ordering::Relaxed);
                self.metrics.rows_out.fetch_add(rows_out, Ordering::Relaxed);
                self.metrics.files_written.fetch_add(nfiles as u64, Ordering::Relaxed);
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or(Duration::ZERO).as_secs();
                self.metrics.last_commit_unix.store(now, Ordering::Relaxed);
                info!(worker = self.id.index, version = v, rows = rows_out, files = nfiles, "batch committed");
                Ok(Step::Processed { more: false })
            }
            None => {
                self.metrics.progress_conflicts.fetch_add(1, Ordering::Relaxed);
                warn!(
                    worker = self.id.index,
                    "another process committed progress for this worker's input; abandoning the batch \
                     ({nfiles} uncommitted files will be removed by VACUUM)"
                );
                Ok(Step::Conflict)
            }
        }
    }
}

/// Partition columns of the partial-state table: the group keys that sink partitions are derived
/// from (partition-scoped merge), else none.
pub fn partials_partition_columns(p: &Pipeline) -> Vec<String> {
    match &p.plan.merge_scope {
        Some(planner::MergeScope::Partitions(cols)) => cols.iter().map(|(_, k)| format!("s{k}")).collect(),
        _ => Vec::new(),
    }
}

/// Object-store URL under which source `i`'s table root is registered.
pub fn store_url(i: usize) -> Result<ObjectStoreUrl> {
    Ok(ObjectStoreUrl::parse(format!("nodra://s{i}"))?)
}

/// Register Delta source `i` with the given files. `t` may be an unloaded handle; the layout is
/// taken from `layout`, or from `t`'s snapshot if `None`.
pub fn register_delta(
    ctx: &SessionContext,
    i: usize,
    t: &DeltaTable,
    layout: Option<&delta::Layout>,
    files: &[DataFile],
    driving: bool,
) -> Result<()> {
    let url = store_url(i)?;
    ctx.register_object_store(url.as_ref(), t.object_store());
    let layout = match layout {
        Some(l) => l.clone(),
        None => delta::Layout::of(t)?,
    };
    let mut table = FileListTable::try_new(layout.file_schema, layout.partition_fields, url, files)?;
    if driving {
        table = table.mark_driving();
    }
    ctx.register_table(source_table(i), Arc::new(table))?;
    Ok(())
}

/// Register all broadcast sources (latest snapshot, or the pinned version) except `skip`.
pub async fn register_broadcast(ctx: &SessionContext, p: &Pipeline, skip: &BTreeSet<usize>) -> Result<()> {
    for (i, role) in p.plan.roles.iter().enumerate() {
        if *role != Role::Broadcast || skip.contains(&i) {
            continue;
        }
        let SourceSpec::Delta { uri, version, .. } = &p.job.sources[i] else { continue };
        let t = match version {
            Some(v) => delta::open_version(uri, *v).await?,
            None => delta::open(uri).await?,
        };
        let now = delta::exposed_schema(&t)?;
        if now.fields() != p.infos[i].schema.fields() {
            return Err(NodraError::delta(format!(
                "the schema of {} changed since the job was planned; restart the job",
                p.job.sources[i].describe()
            )));
        }
        let files = delta::active_files(&t)?;
        register_delta(ctx, i, &t, None, &files, false)?;
    }
    Ok(())
}

/// Register empty, marked placeholders for the driving sources and the real broadcast tables
/// (used to build the plan of the merge step and to learn the partial-state schema).
pub async fn register_placeholders(ctx: &SessionContext, p: &Pipeline) -> Result<()> {
    let driving: BTreeSet<usize> = p.plan.driving().into_iter().collect();
    for &i in &driving {
        let schema = partial::mark_driving(&p.infos[i].schema);
        ctx.register_table(source_table(i), Arc::new(MemTable::try_new(schema, vec![vec![]])?))?;
    }
    register_broadcast(ctx, p, &driving).await
}

/// Failure injection for tests: crash a job's next batches after writing data files but before
/// committing them.
#[doc(hidden)]
pub mod testing {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};

    fn armed() -> &'static Mutex<HashMap<String, u32>> {
        static A: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();
        A.get_or_init(Default::default)
    }

    /// Make the next `times` batches of `job` fail right before their commit.
    pub fn fail_before_commit(job: &str, times: u32) {
        armed().lock().expect("failpoints").insert(job.to_string(), times);
    }

    pub(crate) fn should_fail(job: &str) -> bool {
        let mut a = armed().lock().expect("failpoints");
        match a.get_mut(job) {
            Some(n) if *n > 0 => {
                *n -= 1;
                true
            }
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_progress_encoding_roundtrips() {
        let v: i64 = 123_456;
        let chunks: i64 = 77;
        let enc = (v << 24) | chunks;
        assert_eq!((enc >> 24, enc & 0xff_ffff), (v, chunks));
    }
}
