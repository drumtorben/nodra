//! The merge step of class-3 jobs.
//!
//! Workers commit partial aggregate states to the partials table. The merge step finalizes them
//! with DataFusion's `Final` aggregate (see [`crate::partial`]) and replaces the affected sink
//! data in one commit that also records the partials-table version it merged
//! (`nodra/<job>/merge`), so running it twice is harmless.

use std::collections::{BTreeSet, HashSet};
use std::sync::Arc;

use datafusion::catalog::TableProvider;
use datafusion::physical_plan::execute_stream;
use deltalake_core::kernel::{Action, Transaction};
use tracing::{debug, info};

use crate::delta::{self, CommitOutcome, DataFile};
use crate::error::Result;
use crate::exec::{partials_location, register_placeholders, Pipeline};
use crate::partial;
use crate::planner::MergeScope;
use crate::scan::FileListTable;
use crate::session;
use crate::sink;

/// Result of a merge.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MergeOutcome {
    /// No partial states yet, or nothing new since the last merge.
    UpToDate,
    /// Merged up to this partials-table version.
    Merged {
        /// Partials-table version merged.
        partials_version: i64,
        /// Sink partitions recomputed (`None` = full recompute).
        partitions: Option<usize>,
        /// Rows written.
        rows: u64,
    },
    /// Another merge committed first.
    Conflict,
}

/// Run the merge step once.
pub async fn merge(p: &Pipeline) -> Result<MergeOutcome> {
    let Some(partials) = delta::try_open(&partials_location(&p.job)).await? else {
        return Ok(MergeOutcome::UpToDate);
    };
    let mut sink = delta::create_or_open(p.job.sink.uri(), &p.sink_schema, p.job.sink.partition_by()).await?;
    let key = p.merge_progress_key();
    let merged = delta::txn_version(&sink, &key).await?;
    let latest = delta::version(&partials)? as i64;
    // `>=`: a concurrent merge may already have merged a newer partials version than the
    // snapshot opened here; merging this older one would move the sink backwards.
    if merged.is_some_and(|m| m >= latest) {
        return Ok(MergeOutcome::UpToDate);
    }
    let all_files = delta::active_files(&partials)?;
    let sink_parts = delta::partition_columns(&sink)?;

    // Which partial files to read and which sink files to replace.
    let scoped: Option<HashSet<Vec<Option<String>>>> = match (&p.plan.merge_scope, merged) {
        (Some(MergeScope::Partitions(_)), Some(m)) if m < latest => {
            match delta::changes(&partials, &delta::partition_columns(&partials)?, m as u64, latest as u64).await {
                Ok(changes) => Some(
                    changes
                        .into_iter()
                        .flat_map(|c| c.adds.into_iter().map(|f| f.partition_values))
                        .collect(),
                ),
                // Log entries cleaned up: fall back to a full recompute.
                Err(_) => None,
            }
        }
        _ => None,
    };
    let files: Vec<DataFile> = match &scoped {
        None => all_files,
        Some(affected) => all_files.into_iter().filter(|f| affected.contains(&f.partition_values)).collect(),
    };
    // The sink files the merge replaces, as of `sink` (recomputed if the sink moves on).
    let removes = |sink: &deltalake_core::DeltaTable| -> Result<Vec<Action>> {
        match &scoped {
            None => delta::removes_for(sink, |_| false),
            Some(affected) => delta::removes_for(sink, |pv| {
                let values: Vec<Option<String>> = sink_parts.iter().map(|c| pv.get(c).cloned().flatten()).collect();
                !affected.contains(&values)
            }),
        }
    };

    let ctx = session::new_context(&p.job.settings)?;
    register_placeholders(&ctx, p).await?;
    let physical = ctx.sql(&p.job.query).await?.create_physical_plan().await?;
    let url = datafusion::execution::object_store::ObjectStoreUrl::parse("nodra://partials")?;
    ctx.register_object_store(url.as_ref(), partials.object_store());
    let layout = delta::Layout::of(&partials)?;
    let states = FileListTable::try_new(layout.file_schema, layout.partition_fields, url, &files)?;
    let states_plan = states.scan(&ctx.state(), None, &[], None).await?;
    let plan = partial::merge_plan(&physical, states_plan, &p.plan.split_group_names)?;
    let stream = execute_stream(plan, ctx.task_ctx())?;
    let written = sink::write_stream(&sink, &delta::arrow_schema(&sink)?, sink::Columns::ByName, stream).await?;
    let rows = written.rows;
    let adds: Vec<Action> = written.adds.into_iter().map(Action::Add).collect();
    let predicate = scoped.as_ref().map(|s| format!("nodra merge of {} partition(s)", s.len()));
    let op = delta::overwrite_op(&sink_parts, predicate);
    // The removes are only valid on the snapshot they were computed from: commit without
    // delta-rs retries (which would accept e.g. a concurrent OPTIMIZE that rewrote the files we
    // remove, and keep the old rows), and recompute them on a newer snapshot instead.
    for attempt in 0..20 {
        let mut actions = removes(&sink)?;
        actions.extend(adds.iter().cloned());
        match delta::commit(&mut sink, actions, vec![Transaction::new(&key, latest)], op.clone(), 0).await? {
            CommitOutcome::Committed(v) => {
                info!(
                    sink_version = v,
                    partials_version = latest,
                    partitions = ?scoped.as_ref().map(HashSet::len),
                    rows,
                    "merged"
                );
                return Ok(MergeOutcome::Merged { partials_version: latest, partitions: scoped.map(|s| s.len()), rows });
            }
            CommitOutcome::Conflict => {
                debug!("another merge committed first");
                return Ok(MergeOutcome::Conflict);
            }
            CommitOutcome::Retry(why) => debug!(attempt, "merge conflicts with a concurrent change: {why}"),
            CommitOutcome::Stale => debug!(attempt, "the sink moved on; recomputing the merge commit"),
        }
        sink = delta::open(p.job.sink.uri()).await?;
        if delta::txn_version(&sink, &key).await? != merged {
            debug!("another merge committed first");
            return Ok(MergeOutcome::Conflict);
        }
    }
    Err(crate::NodraError::delta("the merge commit kept conflicting with concurrent changes to the sink"))
}

/// Partition values of the partials files added by `changes` (for tests and status output).
pub fn affected_partitions(changes: &[delta::CommitChanges]) -> BTreeSet<Vec<Option<String>>> {
    changes.iter().flat_map(|c| c.adds.iter().map(|f| f.partition_values.clone())).collect()
}

// Compile-time check that the merge future can run on a multi-threaded runtime.
#[allow(unused, clippy::manual_async_fn)]
fn _assert_send(p: Arc<Pipeline>) -> impl std::future::Future<Output = Result<MergeOutcome>> + Send {
    async move { merge(&p).await }
}
