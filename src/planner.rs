//! Job classification (`nodra plan --explain`).
//!
//! The planner builds DataFusion's optimized logical plan for the job's query, picks the
//! *driving* source (the one split across workers), and checks that every operator between the
//! driving source and the sink can run on a subset of the input without exchanging rows with
//! other workers. See `docs/design.md`, section 2.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::MemTable;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::{Column, JoinType};
use datafusion::logical_expr::{Expr, LogicalPlan};

use crate::error::{NodraError, Result};
use crate::job::{source_table, Job, SourceSpec};
use crate::session;

/// What the planner knows about a source before planning.
#[derive(Debug, Clone)]
pub struct SourceInfo {
    /// Full table schema (data columns followed by partition columns for Delta).
    pub schema: SchemaRef,
    /// Partition columns (Delta).
    pub partition_columns: Vec<String>,
    /// Total size of the active files in bytes (Delta).
    pub size_bytes: Option<u64>,
    /// Number of active files (Delta).
    pub num_files: Option<usize>,
    /// Table version the info was taken from (Delta).
    pub version: Option<i64>,
}

/// The job classes of `docs/design.md`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobClass {
    /// Class 1: one driving source, row-wise operators only.
    PartitionParallel,
    /// Class 2: the driving source joined with broadcast tables.
    BroadcastJoin,
    /// Class 3: aggregation over the driving source with a merge step.
    MergeableAggregation,
    /// Class 4: two large Delta tables joined on their common partition columns.
    CoPartitionedJoin,
}

impl JobClass {
    /// Class number as used in the docs.
    pub fn number(self) -> u8 {
        match self {
            JobClass::PartitionParallel => 1,
            JobClass::BroadcastJoin => 2,
            JobClass::MergeableAggregation => 3,
            JobClass::CoPartitionedJoin => 4,
        }
    }

    /// Display name.
    pub fn name(self) -> &'static str {
        match self {
            JobClass::PartitionParallel => "partition-parallel",
            JobClass::BroadcastJoin => "broadcast join",
            JobClass::MergeableAggregation => "mergeable aggregation",
            JobClass::CoPartitionedJoin => "co-partitioned join",
        }
    }
}

/// How work is divided among workers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UnitKey {
    /// Stream partitions, `partition mod n`.
    StreamPartition,
    /// Delta data files, `fnv(path) mod n`.
    File,
    /// Delta partitions, `fnv(partition values) mod n` (aligned aggregation, class 4).
    Partition(Vec<String>),
}

/// Scope of the class-3 merge step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeScope {
    /// Recompute only the sink partitions that received new partial states. Each entry is a sink
    /// partition column and the index of the split aggregate's group key it is derived from.
    Partitions(Vec<(String, usize)>),
    /// Recompute the whole sink.
    Full,
}

/// Role of a source in the plan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    /// Split across workers.
    Driving,
    /// Loaded fully by every worker.
    Broadcast,
}

/// Planner output.
#[derive(Debug, Clone)]
pub struct JobPlan {
    /// Job class.
    pub class: JobClass,
    /// Role per source (same order as `Job::sources`).
    pub roles: Vec<Role>,
    /// Work-unit assignment.
    pub unit_key: UnitKey,
    /// Schema of the query result.
    pub output_schema: SchemaRef,
    /// Class 3: merge scope.
    pub merge_scope: Option<MergeScope>,
    /// Class 3: names of the split aggregate's group columns, in logical order (the canonical
    /// layout of partial states, see [`crate::partial`]).
    pub split_group_names: Vec<String>,
    /// Human-readable notes collected while planning.
    pub notes: Vec<String>,
}

impl JobPlan {
    /// Indices of the driving sources.
    pub fn driving(&self) -> Vec<usize> {
        self.roles.iter().enumerate().filter(|(_, r)| **r == Role::Driving).map(|(i, _)| i).collect()
    }

    /// Indices of the broadcast sources.
    pub fn broadcast(&self) -> Vec<usize> {
        self.roles.iter().enumerate().filter(|(_, r)| **r == Role::Broadcast).map(|(i, _)| i).collect()
    }
}

/// How earlier runs of a job split their input, read from the progress keys in its target.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Previous {
    /// Delta source `i` was the driving source (classes 1–3).
    Driving(usize),
    /// The job ran as a co-partitioned join (class 4).
    CoPartitioned,
}

/// Classify `job`. `infos[i]` describes `job.sources[i]`.
pub async fn plan_job(job: &Job, infos: &[SourceInfo]) -> Result<JobPlan> {
    plan_job_with(job, infos, None).await
}

/// Like [`plan_job`], keeping the way earlier runs split the input (`previous`): switching the
/// driving source or the class would process the input again under new progress keys and
/// duplicate the output.
pub async fn plan_job_with(job: &Job, infos: &[SourceInfo], previous: Option<Previous>) -> Result<JobPlan> {
    let threshold = job.settings.broadcast_threshold()?;
    let ctx = session::new_context(&job.settings)?;
    for (i, info) in infos.iter().enumerate() {
        let table = MemTable::try_new(info.schema.clone(), vec![vec![]])?;
        ctx.register_table(source_table(i), Arc::new(table))?;
    }
    let df = ctx
        .sql(&job.query)
        .await
        .map_err(|e| NodraError::job(format!("cannot plan query: {e}")))?;
    let output_schema: SchemaRef = Arc::new(df.schema().as_arrow().clone());
    // Physical planning surfaces queries DataFusion cannot execute before any worker starts.
    df.clone()
        .create_physical_plan()
        .await
        .map_err(|e| NodraError::job(format!("cannot plan query: {e}")))?;
    let plan = df.into_optimized_plan()?;
    let mut notes = Vec::new();

    // ---- driving source selection -------------------------------------------------------
    let mut roles = vec![Role::Broadcast; job.sources.len()];
    let mut large: Vec<usize> = Vec::new();
    if let Some(s) = job.stream_source() {
        roles[s] = Role::Driving;
        notes.push(format!("{} is the driving source (stream)", source_table(s)));
        for (i, spec) in job.sources.iter().enumerate() {
            if i != s && !broadcastable(spec, &infos[i], threshold) {
                return Err(NodraError::rejected(
                    format!(
                        "{} ({}) is too large to broadcast ({} > {})",
                        source_table(i),
                        spec.describe(),
                        human_bytes(infos[i].size_bytes.unwrap_or(0)),
                        human_bytes(threshold)
                    ),
                    "joining a stream with a large table needs a shuffle; raise nodra.broadcast_threshold or set broadcast => true if it fits in memory",
                ));
            }
        }
    } else {
        // Largest first; ties in order of appearance.
        let mut by_size: Vec<usize> = (0..job.sources.len()).collect();
        by_size.sort_by_key(|i| std::cmp::Reverse(infos[*i].size_bytes.unwrap_or(0)));
        let forced_broadcast = |i: usize| matches!(job.sources[i], SourceSpec::Delta { broadcast: Some(true), .. });
        // Tables on the non-preserved side of an outer, semi or anti join, or read only in a
        // subquery, cannot be split: every worker would see all rows of the other side.
        let secondary = secondary_tables(&plan);
        let previous_driving = match previous {
            Some(Previous::Driving(i)) => Some(i),
            _ => None,
        };
        let driving = match previous_driving {
            Some(i) if forced_broadcast(i) => {
                return Err(NodraError::job(format!(
                    "{} ({}) was the driving source of earlier runs and cannot be broadcast now; \
                     start the job under a new nodra.job_name to change the driving source",
                    source_table(i),
                    job.sources[i].describe()
                )))
            }
            Some(i) => {
                notes.push(format!("{} is the driving source of earlier runs", source_table(i)));
                i
            }
            None => *by_size
                .iter()
                .find(|i| !forced_broadcast(**i) && !secondary.contains(&source_table(**i)))
                .or_else(|| by_size.iter().find(|i| !forced_broadcast(**i)))
                .unwrap_or(&by_size[0]),
        };
        large.push(driving);
        let largest = by_size[0];
        if largest != driving && !forced_broadcast(largest) && secondary.contains(&source_table(largest)) {
            notes.push(format!(
                "{} is the driving source because {} is on the non-preserved side of a join or in a subquery; \
                 {} is broadcast as of each batch",
                source_table(driving),
                source_table(largest),
                source_table(largest)
            ));
        }
        for &i in &by_size {
            let large_now = !broadcastable(&job.sources[i], &infos[i], threshold);
            // A class-4 job stays one: its second table keeps being split by partition.
            let was_copartitioned = previous == Some(Previous::CoPartitioned) && large.len() == 1 && !forced_broadcast(i);
            if i != driving && (large_now || was_copartitioned) {
                large.push(i);
            }
        }
        if previous == Some(Previous::CoPartitioned) && large.len() < 2 {
            return Err(NodraError::job(
                "earlier runs of this job were a co-partitioned join of two tables; start it under a new nodra.job_name",
            ));
        }
        if previous_driving.is_some() && large.len() > 1 {
            let i = large[1];
            return Err(NodraError::job(format!(
                "{} ({}) has grown beyond nodra.broadcast_threshold ({} > {}), but earlier runs of this job broadcast it; \
                 raise nodra.broadcast_threshold or set broadcast => true to keep the job as it is, or start it under a \
                 new nodra.job_name",
                source_table(i),
                job.sources[i].describe(),
                human_bytes(infos[i].size_bytes.unwrap_or(0)),
                human_bytes(threshold)
            )));
        }
        for &i in &large {
            roles[i] = Role::Driving;
        }
        if large.len() > 2 {
            return Err(NodraError::rejected(
                format!("{} tables are too large to broadcast", large.len()),
                "Nodra can co-partition at most two large tables; run this job on Spark or Sail",
            ));
        }
    }

    // ---- plan walk ------------------------------------------------------------------------
    let driving_tables: BTreeSet<String> = roles
        .iter()
        .enumerate()
        .filter(|(_, r)| **r == Role::Driving)
        .map(|(i, _)| source_table(i))
        .collect();
    for (i, role) in roles.iter().enumerate() {
        if *role != Role::Driving {
            continue;
        }
        if let Some(j) = (0..job.sources.len()).find(|j| *j != i && job.sources[*j].key() == job.sources[i].key()) {
            return Err(NodraError::rejected(
                format!("{} and {} read the same table, which is the driving source", source_table(i), source_table(j)),
                "a second read of the driving source would see all of its rows on every worker; \
                 refer to it once (aggregate or deduplicate in a separate job)",
            ));
        }
        let scans = count_scans(&plan, &source_table(i));
        if scans > 1 {
            return Err(NodraError::rejected(
                format!(
                    "the driving source {} ({}) is read {scans} times (a self-join, a CTE used twice, or a subquery over it)",
                    source_table(i),
                    job.sources[i].describe()
                ),
                "each worker sees only its share of the driving source, so it can be read only once",
            ));
        }
    }
    let mut walk = Walk { driving: &driving_tables, aggregate: None, copartition_join: None };
    let top_has_driving = walk.visit(&plan)?;
    if !top_has_driving {
        return Err(NodraError::job("the query does not read its driving source"));
    }
    reject_subqueries_over(&plan, &driving_tables)?;

    // ---- class --------------------------------------------------------------------------
    let has_broadcast_join = roles.contains(&Role::Broadcast)
        && job.sources.len() > 1;
    let (mut class, unit_key) = if large.len() == 2 {
        let join = walk.copartition_join.clone().ok_or_else(|| {
            NodraError::rejected(
                format!(
                    "{} and {} are both too large to broadcast and are not joined",
                    source_table(large[0]),
                    source_table(large[1])
                ),
                "co-partitioned joins need an equi-join on the partition columns of both tables",
            )
        })?;
        let pairs = copartition_columns(&join, &large, infos)?;
        let cols: Vec<String> = pairs.iter().map(|(a, _)| a.clone()).collect();
        notes.push(format!("co-partitioned on [{}]", cols.join(", ")));
        // Each worker runs the whole query on its partitions, so everything above the join
        // must be partition-local too.
        let keys: Vec<[(String, String); 2]> = pairs
            .iter()
            .map(|(a, b)| [(source_table(large[0]), a.clone()), (source_table(large[1]), b.clone())])
            .collect();
        check_partition_local(&plan, &join, &keys, &mut notes)?;
        (JobClass::CoPartitionedJoin, UnitKey::Partition(cols))
    } else if job.stream_source().is_some() {
        (if has_broadcast_join { JobClass::BroadcastJoin } else { JobClass::PartitionParallel }, UnitKey::StreamPartition)
    } else {
        (if has_broadcast_join { JobClass::BroadcastJoin } else { JobClass::PartitionParallel }, UnitKey::File)
    };

    let mut merge_scope = None;
    let mut split_group_names = Vec::new();
    if let Some(agg) = &walk.aggregate {
        if class == JobClass::CoPartitionedJoin {
            // Checked by `check_partition_local`: class 4 processes every partition once and
            // completely, so an aggregation grouping by the partition columns is final.
        } else {
            // Incremental input means a group can receive rows in many batches and on many
            // workers, so every other aggregation is finalized by the merge step.
            class = JobClass::MergeableAggregation;
            merge_scope = Some(merge_scope_for(&plan, job.sink.partition_by(), agg));
            let n = agg.schema.fields().len() - agg.aggr_expr.len();
            split_group_names = agg.schema.fields().iter().take(n).map(|f| f.name().clone()).collect();
            let unique: BTreeSet<&String> = split_group_names.iter().collect();
            if unique.len() != split_group_names.len() {
                return Err(NodraError::rejected(
                    format!("the aggregation has several group keys with the same name ({})", split_group_names.join(", ")),
                    "alias them in a subquery so that every group key has its own name",
                ));
            }
        }
    }
    if class == JobClass::CoPartitionedJoin && job.stream_source().is_some() {
        return Err(NodraError::rejected("co-partitioned join with a stream", "stream joins need broadcast tables"));
    }
    Ok(JobPlan { class, roles, unit_key, output_schema, merge_scope, split_group_names, notes })
}

fn broadcastable(spec: &SourceSpec, info: &SourceInfo, threshold: u64) -> bool {
    match spec {
        SourceSpec::Delta { broadcast: Some(b), .. } => *b,
        SourceSpec::Delta { .. } => info.size_bytes.unwrap_or(u64::MAX) <= threshold,
        _ => false,
    }
}

/// Render a size in binary units.
pub fn human_bytes(b: u64) -> String {
    const U: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    let mut v = b as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{b} B")
    } else {
        format!("{v:.1} {}", U[i])
    }
}

/// Facts collected while walking the plan.
struct Walk<'a> {
    driving: &'a BTreeSet<String>,
    /// The lowest aggregate over the driving source (the split point).
    aggregate: Option<datafusion::logical_expr::Aggregate>,
    /// The join that combines the two large tables (class 4).
    copartition_join: Option<datafusion::logical_expr::Join>,
}

impl Walk<'_> {
    /// Returns whether `plan` reads a driving source. Rejects operators that would need rows of
    /// other workers, *below* the first aggregate over the driving source.
    fn visit(&mut self, plan: &LogicalPlan) -> Result<bool> {
        match plan {
            LogicalPlan::TableScan(scan) => {
                let name = scan.table_name.table().to_string();
                if self.driving.contains(&name) && scan.fetch.is_some() {
                    return Err(limit_rejection());
                }
                Ok(self.driving.contains(&name))
            }
            LogicalPlan::Aggregate(agg) => {
                let below = self.visit(&agg.input)?;
                if below && self.aggregate.is_none() {
                    self.aggregate = Some(agg.clone());
                    // Everything above runs in the merge step (or on finalized partitions),
                    // so it is not restricted further: stop checking upwards.
                    return Ok(true);
                }
                Ok(below)
            }
            LogicalPlan::Join(join) => {
                let l = self.visit(&join.left)?;
                let r = self.visit(&join.right)?;
                if self.aggregate.is_some() {
                    return Ok(l || r);
                }
                match (l, r) {
                    (true, true) => {
                        if self.copartition_join.is_some() {
                            return Err(NodraError::rejected(
                                "the large tables are joined more than once",
                                "run this job on Spark or Sail",
                            ));
                        }
                        if join.join_type != JoinType::Inner {
                            return Err(NodraError::rejected(
                                format!("{} join between two large tables", join.join_type),
                                "only inner joins can be co-partitioned",
                            ));
                        }
                        self.copartition_join = Some(join.clone());
                    }
                    (true, false) => check_join_side(join.join_type, true)?,
                    (false, true) => check_join_side(join.join_type, false)?,
                    (false, false) => {}
                }
                Ok(l || r)
            }
            LogicalPlan::Window(w) => {
                let below = self.visit(&w.input)?;
                if below && self.aggregate.is_none() {
                    return Err(NodraError::rejected(
                        "window function over the driving source",
                        "window functions need all rows of a window on one worker; aggregate first or run on Spark",
                    ));
                }
                Ok(below)
            }
            LogicalPlan::Limit(l) => {
                let below = self.visit(&l.input)?;
                if below && self.aggregate.is_none() {
                    return Err(limit_rejection());
                }
                Ok(below)
            }
            LogicalPlan::Union(u) => {
                let mut any = false;
                for input in &u.inputs {
                    any |= self.visit(input)?;
                }
                if any && self.aggregate.is_none() {
                    return Err(NodraError::rejected(
                        "UNION involving the driving source",
                        "not supported yet; write each branch with its own job into the same table",
                    ));
                }
                Ok(any)
            }
            LogicalPlan::Distinct(d) => {
                let below = self.visit(d.input())?;
                if below && self.aggregate.is_none() {
                    return Err(NodraError::rejected(
                        "DISTINCT ON over the driving source",
                        "use GROUP BY instead",
                    ));
                }
                Ok(below)
            }
            LogicalPlan::Sort(sort) if sort.fetch.is_some() => {
                // `ORDER BY … LIMIT n` is planned as a sort with a fetch, without a Limit node.
                let below = self.visit(&sort.input)?;
                if below && self.aggregate.is_none() {
                    return Err(limit_rejection());
                }
                Ok(below)
            }
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::Sort(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Unnest(_)
            | LogicalPlan::Subquery(_)
            | LogicalPlan::Repartition(_) => {
                let mut any = false;
                for input in plan.inputs() {
                    any |= self.visit(input)?;
                }
                Ok(any)
            }
            LogicalPlan::EmptyRelation(_) | LogicalPlan::Values(_) => Ok(false),
            other => Err(NodraError::rejected(
                format!("unsupported plan node `{}`", other.display()),
                "Nodra supports SELECT queries with filters, projections, joins and aggregations",
            )),
        }
    }
}

/// Number of scans of `table` in `plan`, including subqueries.
fn count_scans(plan: &LogicalPlan, table: &str) -> usize {
    tables(plan).iter().filter(|t| *t == table).count()
}

/// Names of all scanned tables, one entry per scan, including subqueries.
fn tables(plan: &LogicalPlan) -> Vec<String> {
    let mut out = Vec::new();
    let _ = plan.apply_with_subqueries(|n| {
        if let LogicalPlan::TableScan(s) = n {
            out.push(s.table_name.table().to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

/// Tables that cannot be the driving source: those on the non-preserved side of an outer, semi
/// or anti join, and those read in subqueries.
fn secondary_tables(plan: &LogicalPlan) -> BTreeSet<String> {
    use JoinType::*;
    let mut out = BTreeSet::new();
    let _ = plan.apply_with_subqueries(|n| {
        if let LogicalPlan::Join(j) = n {
            let sides: &[&LogicalPlan] = match j.join_type {
                Inner => &[],
                Left | LeftSemi | LeftAnti | LeftMark => &[&j.right],
                Right | RightSemi | RightAnti | RightMark => &[&j.left],
                Full => &[&j.left, &j.right],
            };
            for side in sides {
                out.extend(tables(side));
            }
        }
        let _ = n.apply_subqueries(|sub| {
            out.extend(tables(&sub));
            Ok(TreeNodeRecursion::Continue)
        });
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

/// Class 4: check the operators between the root and the co-partitioned join. They run per
/// partition on each worker, so only row-wise operators, joins with broadcast tables and
/// aggregations that group by the partition columns are allowed. `keys[k]` names partition
/// column `k` in both tables.
fn check_partition_local(
    plan: &LogicalPlan,
    join: &datafusion::logical_expr::Join,
    keys: &[[(String, String); 2]],
    notes: &mut Vec<String>,
) -> Result<()> {
    let reject = |what: String| {
        NodraError::rejected(
            format!("{what} above a co-partitioned join"),
            "each worker sees only its own partitions; write the join result and post-process it in a second job",
        )
    };
    let mut node = plan;
    loop {
        if let LogicalPlan::Join(j) = node {
            if j == join {
                return Ok(());
            }
        }
        let next = match node {
            LogicalPlan::Projection(_)
            | LogicalPlan::Filter(_)
            | LogicalPlan::SubqueryAlias(_)
            | LogicalPlan::Repartition(_)
            | LogicalPlan::Unnest(_) => node.inputs()[0],
            LogicalPlan::Sort(s) if s.fetch.is_none() => &s.input,
            LogicalPlan::Aggregate(agg) => {
                let covered = keys.iter().all(|k| group_covers(agg, k));
                if !covered {
                    return Err(NodraError::rejected(
                        "aggregation over a co-partitioned join that does not group by the partition columns",
                        "group by the partition columns, or write the join result and aggregate it in a second job",
                    ));
                }
                notes.push("aggregation groups by the partition columns: each worker finalizes its own partitions".into());
                &agg.input
            }
            LogicalPlan::Join(j) => {
                let l = contains_join(&j.left, join);
                check_join_side(j.join_type, l)?;
                if l {
                    &j.left
                } else {
                    &j.right
                }
            }
            LogicalPlan::Sort(_) | LogicalPlan::Limit(_) => return Err(reject("LIMIT".into())),
            LogicalPlan::Window(_) => return Err(reject("a window function".into())),
            other => return Err(reject(format!("`{}`", other.display()))),
        };
        node = next;
    }
}

fn contains_join(plan: &LogicalPlan, join: &datafusion::logical_expr::Join) -> bool {
    let mut found = false;
    let _ = plan.apply(|n| {
        if let LogicalPlan::Join(j) = n {
            if j == join {
                found = true;
                return Ok(TreeNodeRecursion::Stop);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    });
    found
}

fn limit_rejection() -> NodraError {
    NodraError::rejected(
        "LIMIT over the driving source",
        "a global LIMIT needs all rows on one worker; aggregate first or filter instead",
    )
}

/// `driving_left`: the driving source is on the left side of the join.
fn check_join_side(t: JoinType, driving_left: bool) -> Result<()> {
    use JoinType::*;
    let ok = match t {
        Inner => true,
        Left | LeftSemi | LeftAnti | LeftMark => driving_left,
        Right | RightSemi | RightAnti | RightMark => !driving_left,
        Full => false,
    };
    if ok {
        Ok(())
    } else {
        Err(NodraError::rejected(
            format!("{t} join that preserves or filters the broadcast side"),
            "every worker would emit the unmatched broadcast rows; put the driving source on the preserved side",
        ))
    }
}

fn reject_subqueries_over(plan: &LogicalPlan, driving: &BTreeSet<String>) -> Result<()> {
    let mut bad = false;
    plan.apply_with_subqueries(|p| {
        p.apply_subqueries(|sub| {
            sub.apply(|n| {
                if let LogicalPlan::TableScan(s) = n {
                    if driving.contains(s.table_name.table()) {
                        bad = true;
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        Ok(TreeNodeRecursion::Continue)
    })?;
    if bad {
        return Err(NodraError::rejected(
            "subquery over the driving source",
            "a subquery over the driving source needs all of its rows; join or aggregate instead",
        ));
    }
    Ok(())
}

/// Where an output column of `plan` comes from: `(table, column)` of a table scan.
fn origin(plan: &LogicalPlan, col: &Column) -> Option<(String, String)> {
    match plan {
        LogicalPlan::TableScan(s) => {
            s.projected_schema.index_of_column(col).ok()?;
            Some((s.table_name.table().to_string(), col.name.clone()))
        }
        LogicalPlan::Projection(p) => {
            let i = p.schema.index_of_column(col).ok()?;
            match p.expr.get(i)?.clone().unalias() {
                Expr::Column(c) => origin(&p.input, &c),
                _ => None,
            }
        }
        LogicalPlan::SubqueryAlias(a) => {
            let i = a.schema.index_of_column(col).ok()?;
            let (q, f) = a.input.schema().qualified_field(i);
            origin(&a.input, &Column::new(q.cloned(), f.name()))
        }
        LogicalPlan::Filter(_) | LogicalPlan::Sort(_) | LogicalPlan::Limit(_) | LogicalPlan::Repartition(_) => {
            origin(plan.inputs()[0], col)
        }
        LogicalPlan::Join(j) => {
            if j.left.schema().index_of_column(col).is_ok() {
                origin(&j.left, col)
            } else {
                origin(&j.right, col)
            }
        }
        LogicalPlan::Aggregate(a) => {
            let i = a.schema.index_of_column(col).ok()?;
            match a.group_expr.get(i)?.clone().unalias() {
                Expr::Column(c) => origin(&a.input, &c),
                _ => None,
            }
        }
        _ => None,
    }
}

/// True if one of the aggregate's group keys is one of `names` (`(table, column)` pairs that
/// the co-partitioned join equates).
fn group_covers(agg: &datafusion::logical_expr::Aggregate, names: &[(String, String)]) -> bool {
    agg.group_expr
        .iter()
        .filter_map(|e| match e.clone().unalias() {
            Expr::Column(c) => origin(&agg.input, &c),
            _ => None,
        })
        .any(|o| names.contains(&o))
}

/// The partition columns of both large tables, pairwise: `(column in a, column in b)`.
fn copartition_columns(
    join: &datafusion::logical_expr::Join,
    large: &[usize],
    infos: &[SourceInfo],
) -> Result<Vec<(String, String)>> {
    let (a, b) = (large[0], large[1]);
    let (pa, pb) = (&infos[a].partition_columns, &infos[b].partition_columns);
    let reject = |why: &str| {
        NodraError::rejected(
            format!("{} and {} are too large to broadcast and {why}", source_table(a), source_table(b)),
            "co-partitioned joins need both tables partitioned by the same columns and an equi-join on all of them",
        )
    };
    if pa.is_empty() || pa.len() != pb.len() {
        return Err(reject("are not partitioned alike"));
    }
    let pairs: Vec<((String, String), (String, String))> = join
        .on
        .iter()
        .filter_map(|(l, r)| match (l.clone().unalias(), r.clone().unalias()) {
            (Expr::Column(lc), Expr::Column(rc)) => Some((origin(&join.left, &lc)?, origin(&join.right, &rc)?)),
            _ => None,
        })
        .collect();
    let (ta, tb) = (source_table(a), source_table(b));
    for (ca, cb) in pa.iter().zip(pb) {
        let found = pairs.iter().any(|(l, r)| {
            (l.0 == ta && &l.1 == ca && r.0 == tb && &r.1 == cb) || (l.0 == tb && &l.1 == cb && r.0 == ta && &r.1 == ca)
        });
        if !found {
            return Err(reject(&format!("the join does not equate `{ca}` with `{cb}`")));
        }
        let ta_type = infos[a].schema.field_with_name(ca).map(|f| f.data_type().clone());
        let tb_type = infos[b].schema.field_with_name(cb).map(|f| f.data_type().clone());
        if ta_type.ok() != tb_type.ok() {
            return Err(reject(&format!("partition columns `{ca}` and `{cb}` have different types")));
        }
    }
    Ok(pa.iter().cloned().zip(pb.iter().cloned()).collect())
}

/// Partition-scoped merge is possible when every sink partition column is derived, through
/// row-wise operators (and outer aggregations grouping by it), from a group key of the split
/// aggregate: then all rows of a sink partition come from partial states with that key value.
fn merge_scope_for(
    plan: &LogicalPlan,
    sink_partitions: &[String],
    split: &datafusion::logical_expr::Aggregate,
) -> MergeScope {
    if sink_partitions.is_empty() {
        return MergeScope::Full;
    }
    let schema = plan.schema();
    let mut out = Vec::new();
    for p in sink_partitions {
        let Some((q, f)) = schema.iter().find(|(_, f)| f.name() == p) else {
            return MergeScope::Full;
        };
        match group_key_origin(plan, &Column::new(q.cloned(), f.name()), split) {
            // Two sink partition columns derived from one group key: keep it simple.
            Some(k) if out.iter().any(|(_, o)| *o == k) => return MergeScope::Full,
            Some(k) => out.push((p.clone(), k)),
            None => return MergeScope::Full,
        }
    }
    MergeScope::Partitions(out)
}

/// Follow a column from the top of the plan down to a group key of the split aggregate.
fn group_key_origin(
    plan: &LogicalPlan,
    col: &Column,
    split: &datafusion::logical_expr::Aggregate,
) -> Option<usize> {
    match plan {
        LogicalPlan::Aggregate(a) => {
            let i = a.schema.index_of_column(col).ok()?;
            if i >= a.group_expr.len() || matches!(a.group_expr[i], Expr::GroupingSet(_)) {
                return None;
            }
            if a == split {
                return Some(i);
            }
            match a.group_expr[i].clone().unalias() {
                Expr::Column(c) => group_key_origin(&a.input, &c, split),
                _ => None,
            }
        }
        LogicalPlan::Projection(p) => {
            let i = p.schema.index_of_column(col).ok()?;
            match p.expr.get(i)?.clone().unalias() {
                Expr::Column(c) => group_key_origin(&p.input, &c, split),
                _ => None,
            }
        }
        LogicalPlan::SubqueryAlias(a) => {
            let i = a.schema.index_of_column(col).ok()?;
            let (q, f) = a.input.schema().qualified_field(i);
            group_key_origin(&a.input, &Column::new(q.cloned(), f.name()), split)
        }
        LogicalPlan::Filter(_) => group_key_origin(plan.inputs()[0], col, split),
        // A top-N needs all groups: recompute everything.
        LogicalPlan::Sort(s) if s.fetch.is_none() => group_key_origin(&s.input, col, split),
        _ => None,
    }
}

/// Render the plan for `nodra plan --explain`.
pub fn explain(job: &Job, infos: &[SourceInfo], plan: &JobPlan) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "job:     {}", job.name);
    let _ = writeln!(s, "class:   {} · {}", plan.class.number(), plan.class.name());
    let _ = writeln!(s, "sources:");
    for (i, spec) in job.sources.iter().enumerate() {
        let info = &infos[i];
        let role = match (&plan.roles[i], spec.is_stream()) {
            (Role::Driving, true) => "driving · partitions assigned by `partition mod workers`".to_string(),
            (Role::Driving, false) => match &plan.unit_key {
                UnitKey::Partition(cols) => format!("driving · partitions [{}] assigned by hash", cols.join(", ")),
                _ => "driving · files assigned by hash".to_string(),
            },
            (Role::Broadcast, _) => "broadcast · loaded by every worker".to_string(),
        };
        let size = match (info.size_bytes, info.num_files) {
            (Some(b), Some(n)) => format!(" ({}, {n} file{})", human_bytes(b), if n == 1 { "" } else { "s" }),
            _ => String::new(),
        };
        let _ = writeln!(s, "  {}  {}{size}\n      {role}", source_table(i), spec.describe());
    }
    let parts = job.sink.partition_by();
    let _ = writeln!(
        s,
        "sink:    delta('{}'){}",
        job.sink.uri(),
        if parts.is_empty() { String::new() } else { format!(" partitioned by [{}]", parts.join(", ")) }
    );
    let _ = writeln!(s, "output:");
    let width = plan.output_schema.fields().iter().map(|f| f.name().len()).max().unwrap_or(0);
    for f in plan.output_schema.fields() {
        let stored = match crate::types::delta_type(f.data_type()) {
            Ok(t) if &t != f.data_type() => format!("  (stored as {})", display_type(&t)),
            Ok(_) => String::new(),
            Err(e) => format!("  ({e})"),
        };
        let _ = writeln!(s, "  {:width$}  {}{stored}", f.name(), display_type(f.data_type()));
    }
    if let Some(scope) = &plan.merge_scope {
        let _ = writeln!(
            s,
            "merge:   partial states → {}; {}",
            crate::exec::partials_location(job),
            match scope {
                MergeScope::Full => "full recompute of the sink".to_string(),
                MergeScope::Partitions(p) => format!(
                    "recompute changed sink partitions [{}]",
                    p.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>().join(", ")
                ),
            }
        );
    }
    for n in &plan.notes {
        let _ = writeln!(s, "note:    {n}");
    }
    s
}

fn display_type(dt: &DataType) -> String {
    format!("{dt}")
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::datatypes::{Field, Schema};

    use super::*;

    fn info(cols: &[(&str, DataType)], parts: &[&str], size: u64) -> SourceInfo {
        SourceInfo {
            schema: Arc::new(Schema::new(
                cols.iter().map(|(n, t)| Field::new(*n, t.clone(), true)).collect::<Vec<_>>(),
            )),
            partition_columns: parts.iter().map(|s| s.to_string()).collect(),
            size_bytes: Some(size),
            num_files: Some(1),
            version: Some(0),
        }
    }

    fn events(size: u64) -> SourceInfo {
        info(
            &[("ts", DataType::Int64), ("cid", DataType::Int64), ("v", DataType::Float64), ("day", DataType::Utf8)],
            &["day"],
            size,
        )
    }

    fn dim() -> SourceInfo {
        info(&[("id", DataType::Int64), ("brand", DataType::Utf8)], &[], 1 << 20)
    }

    const BIG: u64 = 10 << 30;

    async fn plan(sql: &str, infos: Vec<SourceInfo>) -> Result<JobPlan> {
        let job = Job::parse(sql, "t")?;
        plan_job(&job, &infos).await
    }

    #[tokio::test]
    async fn class1_filter_projection() {
        let p = plan("INSERT INTO delta('/o') SELECT cid, v * 2 AS v2 FROM delta('/e') WHERE v > 1", vec![events(BIG)])
            .await
            .unwrap();
        assert_eq!(p.class, JobClass::PartitionParallel);
        assert_eq!(p.unit_key, UnitKey::File);
        assert_eq!(p.output_schema.fields().len(), 2);
    }

    #[tokio::test]
    async fn class2_broadcast_join() {
        let p = plan(
            "INSERT INTO delta('/o') SELECT e.ts, d.brand FROM delta('/e') e JOIN delta('/d') d ON e.cid = d.id",
            vec![events(BIG), dim()],
        )
        .await
        .unwrap();
        assert_eq!(p.class, JobClass::BroadcastJoin);
        assert_eq!(p.roles, vec![Role::Driving, Role::Broadcast]);
        let p = plan(
            "INSERT INTO delta('/o') SELECT e.ts, d.brand FROM delta('/e') e LEFT JOIN delta('/d') d ON e.cid = d.id",
            vec![events(BIG), dim()],
        )
        .await
        .unwrap();
        assert_eq!(p.class, JobClass::BroadcastJoin);
    }

    #[tokio::test]
    async fn class3_aggregation_and_scope() {
        let p = plan(
            "INSERT INTO delta('/o') SELECT d.brand, count(*) AS n, avg(e.v) AS av \
             FROM delta('/e') e JOIN delta('/d') d ON e.cid = d.id GROUP BY d.brand",
            vec![events(BIG), dim()],
        )
        .await
        .unwrap();
        assert_eq!(p.class, JobClass::MergeableAggregation);
        assert_eq!(p.merge_scope, Some(MergeScope::Full));
        let p = plan(
            "INSERT INTO delta('/o', partition_by => 'brand') SELECT d.brand, count(*) AS n \
             FROM delta('/e') e JOIN delta('/d') d ON e.cid = d.id GROUP BY d.brand HAVING count(*) > 1",
            vec![events(BIG), dim()],
        )
        .await
        .unwrap();
        assert_eq!(p.merge_scope, Some(MergeScope::Partitions(vec![("brand".into(), 0)])));
    }

    #[tokio::test]
    async fn incremental_aggregation_always_merges() {
        // Even grouping by the partition column needs a merge: new files of a partition arrive
        // in later batches.
        let p = plan(
            "INSERT INTO delta('/o', partition_by => 'day') SELECT day, cid, sum(v) AS s FROM delta('/e') GROUP BY day, cid",
            vec![events(BIG)],
        )
        .await
        .unwrap();
        assert_eq!(p.class, JobClass::MergeableAggregation);
        assert_eq!(p.unit_key, UnitKey::File);
        assert_eq!(p.merge_scope, Some(MergeScope::Partitions(vec![("day".into(), 0)])));
    }

    #[tokio::test]
    async fn count_distinct_merge_scope_goes_through_outer_aggregate() {
        let p = plan(
            "INSERT INTO delta('/o', partition_by => 'day') SELECT day, count(DISTINCT cid) AS users FROM delta('/e') GROUP BY day",
            vec![events(BIG)],
        )
        .await
        .unwrap();
        assert_eq!(p.class, JobClass::MergeableAggregation);
        assert!(matches!(p.merge_scope, Some(MergeScope::Partitions(_))), "{:?}", p.merge_scope);
    }

    #[tokio::test]
    async fn class4_aggregation_must_group_by_partition() {
        let other = info(&[("day", DataType::Utf8), ("cid", DataType::Int64), ("w", DataType::Float64)], &["day"], BIG);
        let p = plan(
            "INSERT INTO delta('/o') SELECT a.day, sum(b.w) AS w FROM delta('/a') a JOIN delta('/b') b ON a.day = b.day GROUP BY a.day",
            vec![events(BIG), other.clone()],
        )
        .await
        .unwrap();
        assert_eq!(p.class, JobClass::CoPartitionedJoin);
        let err = plan(
            "INSERT INTO delta('/o') SELECT a.cid, sum(b.w) AS w FROM delta('/a') a JOIN delta('/b') b ON a.day = b.day GROUP BY a.cid",
            vec![events(BIG), other],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, NodraError::Rejected(_)));
    }

    #[tokio::test]
    async fn stream_aggregation_is_class3() {
        let p = plan(
            "INSERT INTO delta('/o') SELECT d.brand, count(*) AS n FROM jsonl_stream('/s', schema => 'cid BIGINT') s \
             JOIN delta('/d') d ON s.cid = d.id GROUP BY d.brand",
            vec![info(&[("cid", DataType::Int64)], &[], 0), dim()],
        )
        .await
        .unwrap();
        assert_eq!(p.class, JobClass::MergeableAggregation);
        assert_eq!(p.unit_key, UnitKey::StreamPartition);
    }

    #[tokio::test]
    async fn class4_copartitioned() {
        let other = info(&[("day", DataType::Utf8), ("cid", DataType::Int64), ("w", DataType::Float64)], &["day"], BIG);
        let p = plan(
            "INSERT INTO delta('/o') SELECT a.day, a.cid, a.v, b.w FROM delta('/a') a \
             JOIN delta('/b') b ON a.day = b.day AND a.cid = b.cid",
            vec![events(BIG), other.clone()],
        )
        .await
        .unwrap();
        assert_eq!(p.class, JobClass::CoPartitionedJoin);
        assert_eq!(p.unit_key, UnitKey::Partition(vec!["day".into()]));
        let err = plan(
            "INSERT INTO delta('/o') SELECT a.day, b.w FROM delta('/a') a JOIN delta('/b') b ON a.cid = b.cid",
            vec![events(BIG), other],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, NodraError::Rejected(_)), "{err}");
    }

    #[tokio::test]
    async fn rejections() {
        let cases = [
            "INSERT INTO delta('/o') SELECT cid, row_number() OVER (PARTITION BY cid ORDER BY ts) AS r FROM delta('/e')",
            "INSERT INTO delta('/o') SELECT cid FROM delta('/e') LIMIT 10",
            "INSERT INTO delta('/o') SELECT e.ts, d.brand FROM delta('/e') e RIGHT JOIN delta('/d') d ON e.cid = d.id",
            "INSERT INTO delta('/o') SELECT e.ts, d.brand FROM delta('/e') e FULL JOIN delta('/d') d ON e.cid = d.id",
            "INSERT INTO delta('/o') SELECT cid FROM delta('/e') UNION ALL SELECT id FROM delta('/d')",
        ];
        for sql in cases {
            let err = plan(sql, vec![events(BIG), dim()]).await.unwrap_err();
            assert!(matches!(err, NodraError::Rejected(_)), "{sql}: {err}");
        }
        let err = plan(
            "INSERT INTO delta('/o') SELECT cid FROM delta('/e') WHERE v > (SELECT avg(v) FROM delta('/e2'))",
            vec![events(BIG), events(BIG)],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, NodraError::Rejected(_)), "{err}");
    }

    #[tokio::test]
    async fn too_large_to_broadcast_with_stream() {
        let err = plan(
            "INSERT INTO delta('/o') SELECT s.cid FROM jsonl_stream('/s', schema => 'cid BIGINT') s JOIN delta('/e') e ON s.cid = e.cid",
            vec![info(&[("cid", DataType::Int64)], &[], 0), events(BIG)],
        )
        .await
        .unwrap_err();
        assert!(matches!(err, NodraError::Rejected(_)), "{err}");
    }

    #[tokio::test]
    async fn driving_source_read_once() {
        let cases = [
            "INSERT INTO delta('/o') WITH x AS (SELECT * FROM delta('/e')) SELECT a.cid FROM x a JOIN x b ON a.cid = b.cid AND a.ts < b.ts",
            "INSERT INTO delta('/o') WITH x AS (SELECT * FROM delta('/e')) SELECT cid FROM x WHERE v > (SELECT avg(v) FROM x)",
            "INSERT INTO delta('/o') WITH x AS (SELECT * FROM delta('/e')) SELECT cid, count(*) AS n FROM x GROUP BY cid UNION ALL SELECT cid, 1 FROM x",
            "INSERT INTO delta('/o') SELECT a.cid FROM delta('/e') a JOIN delta('/e/') b ON a.cid = b.cid",
        ];
        for sql in cases {
            let infos = if sql.contains("delta('/e/')") { vec![events(BIG), events(1 << 20)] } else { vec![events(BIG)] };
            let err = plan(sql, infos).await.unwrap_err();
            assert!(matches!(err, NodraError::Rejected(_)), "{sql}: {err}");
        }
    }

    #[tokio::test]
    async fn top_n_is_a_limit() {
        for sql in [
            "INSERT INTO delta('/o') SELECT cid, v FROM delta('/e') ORDER BY v DESC LIMIT 5",
            "INSERT INTO delta('/o') SELECT cid, count(*) AS n FROM (SELECT * FROM delta('/e') ORDER BY v DESC LIMIT 10) GROUP BY cid",
        ] {
            let err = plan(sql, vec![events(BIG)]).await.unwrap_err();
            assert!(matches!(err, NodraError::Rejected(_)), "{sql}: {err}");
        }
        // Above the aggregate a top-N is fine, but the merge must recompute all partitions.
        let p = plan(
            "INSERT INTO delta('/o', partition_by => 'day') SELECT day, cid, count(*) AS n FROM delta('/e') \
             GROUP BY day, cid ORDER BY n DESC LIMIT 4",
            vec![events(BIG)],
        )
        .await
        .unwrap();
        assert_eq!(p.merge_scope, Some(MergeScope::Full));
    }

    #[tokio::test]
    async fn class4_only_partition_local_operators_above_the_join() {
        let other = info(&[("day", DataType::Utf8), ("cid", DataType::Int64), ("w", DataType::Float64)], &["day"], BIG);
        for sql in [
            "INSERT INTO delta('/o') SELECT count(*) AS n, sum(s) AS s FROM \
             (SELECT a.day, sum(b.w) AS s FROM delta('/a') a JOIN delta('/b') b ON a.day = b.day GROUP BY a.day)",
            "INSERT INTO delta('/o') SELECT a.day, sum(b.w) AS s FROM delta('/a') a JOIN delta('/b') b ON a.day = b.day \
             GROUP BY a.day ORDER BY s DESC LIMIT 2",
            "INSERT INTO delta('/o') SELECT a.day, a.cid, rank() OVER (ORDER BY b.w) AS r FROM delta('/a') a \
             JOIN delta('/b') b ON a.day = b.day",
        ] {
            let err = plan(sql, vec![events(BIG), other.clone()]).await.unwrap_err();
            assert!(matches!(err, NodraError::Rejected(_)), "{sql}: {err}");
        }
        // Grouping by the other table's partition column, and nested per-partition aggregates.
        for sql in [
            "INSERT INTO delta('/o') SELECT b.day, count(*) AS n FROM delta('/a') a JOIN delta('/b') b ON a.day = b.day GROUP BY b.day",
            "INSERT INTO delta('/o') SELECT day, max(s) AS m FROM (SELECT a.day, a.cid, sum(b.w) AS s FROM delta('/a') a \
             JOIN delta('/b') b ON a.day = b.day GROUP BY a.day, a.cid) GROUP BY day",
        ] {
            let p = plan(sql, vec![events(BIG), other.clone()]).await.unwrap();
            assert_eq!(p.class, JobClass::CoPartitionedJoin, "{sql}");
        }
    }

    #[tokio::test]
    async fn driving_source_follows_join_structure_and_history() {
        // The dimension is larger, but the fact table is the preserved side of the LEFT JOIN.
        let small_events = events(1 << 10);
        let sql = "INSERT INTO delta('/o') SELECT e.ts, d.brand FROM delta('/e') e LEFT JOIN delta('/d') d ON e.cid = d.id";
        let p = plan(sql, vec![small_events.clone(), dim()]).await.unwrap();
        assert_eq!(p.roles, vec![Role::Driving, Role::Broadcast]);
        // An inner join picks the largest table, unless earlier runs used another one.
        let sql = "INSERT INTO delta('/o') SELECT e.ts, d.brand FROM delta('/e') e JOIN delta('/d') d ON e.cid = d.id";
        let p = plan(sql, vec![small_events.clone(), dim()]).await.unwrap();
        assert_eq!(p.roles, vec![Role::Broadcast, Role::Driving]);
        let job = Job::parse(sql, "t").unwrap();
        let p = plan_job_with(&job, &[small_events, dim()], Some(Previous::Driving(0))).await.unwrap();
        assert_eq!(p.roles, vec![Role::Driving, Role::Broadcast]);
    }

    #[tokio::test]
    async fn unsupported_queries_fail_at_plan_time() {
        let err = plan(
            "INSERT INTO delta('/o') SELECT e.cid, e.cid IN (SELECT id FROM delta('/d')) AS m FROM delta('/e') e",
            vec![events(BIG), dim()],
        )
        .await
        .unwrap_err();
        assert!(err.to_string().contains("cannot plan query"), "{err}");
    }
}
