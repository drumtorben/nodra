//! Splitting a physical plan at the aggregation over the driving source (class 3).
//!
//! The driving source is registered with the schema-metadata marker [`DRIVING_MARKER`], so its
//! scan can be found in the physical plan after DataFusion's optimizers ran. The aggregate
//! closest to that scan is the *split aggregate*:
//!
//! * workers run it in `Partial` mode and persist its output (accumulator states),
//! * the merge step replaces it with a `Final` aggregate over a scan of those states and runs
//!   the rest of the plan on top.
//!
//! The persisted layout is canonical: group columns in the order of the *logical* aggregate
//! (identified by name), then the accumulator states. Physical optimizers may reorder the group
//! keys differently on workers and in the merge step (for example to match a join above the
//! aggregate), so both sides map their plan's layout to the canonical one.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::repartition::RepartitionExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, Partitioning};

use crate::error::{NodraError, Result};

/// Schema metadata key that marks the driving source.
pub const DRIVING_MARKER: &str = "nodra.driving";

/// Add the driving marker to a schema.
pub fn mark_driving(schema: &SchemaRef) -> SchemaRef {
    let mut md: HashMap<String, String> = schema.metadata().clone();
    md.insert(DRIVING_MARKER.to_string(), "true".to_string());
    Arc::new(Schema::new_with_metadata(schema.fields().clone(), md))
}

fn is_driving_leaf(plan: &Arc<dyn ExecutionPlan>) -> bool {
    plan.children().is_empty() && plan.schema().metadata().contains_key(DRIVING_MARKER)
}

/// Find the aggregate closest to the driving scan.
fn find_split(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    // Returns (contains driving leaf, split found below)
    fn walk(p: &Arc<dyn ExecutionPlan>) -> (bool, Option<Arc<dyn ExecutionPlan>>) {
        if is_driving_leaf(p) {
            return (true, None);
        }
        let mut contains = false;
        for c in p.children() {
            let (has, found) = walk(c);
            if found.is_some() {
                return (true, found);
            }
            contains |= has;
        }
        if contains && p.is::<AggregateExec>() {
            return (true, Some(p.clone()));
        }
        (contains, None)
    }
    walk(plan).1
}

/// The plan a worker runs for a class-3 job: the split aggregate in `Partial` mode, with its
/// output in the canonical layout. `group_names` are the logical aggregate's group columns.
pub fn worker_plan(plan: &Arc<dyn ExecutionPlan>, group_names: &[String]) -> Result<Arc<dyn ExecutionPlan>> {
    let split = find_split(plan).ok_or_else(|| {
        NodraError::job("internal: no aggregate over the driving source found in the physical plan")
    })?;
    let agg = split.downcast_ref::<AggregateExec>().expect("aggregate");
    let partial: Arc<dyn ExecutionPlan> = match agg.mode() {
        AggregateMode::Partial => split.clone(),
        AggregateMode::Single | AggregateMode::SinglePartitioned => Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            agg.group_expr().clone(),
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            agg.input().clone(),
            agg.input_schema(),
        )?),
        other => {
            return Err(NodraError::job(format!(
                "internal: unexpected aggregate mode {other:?} next to the driving source"
            )))
        }
    };
    let order = canonical_order(&partial.schema(), agg.group_expr().num_group_exprs(), group_names)?;
    if order.iter().enumerate().all(|(i, j)| i == *j) {
        return Ok(partial);
    }
    let schema = partial.schema();
    let exprs = order
        .iter()
        .map(|&j| (Arc::new(Column::new(schema.field(j).name(), j)) as Arc<dyn PhysicalExpr>, schema.field(j).name().clone()))
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(exprs, partial)?))
}

/// Indices of `partial`'s fields in canonical order: the first `group_columns` fields reordered
/// like `group_names`, then the remaining (state) fields unchanged.
pub fn canonical_order(partial: &SchemaRef, group_columns: usize, group_names: &[String]) -> Result<Vec<usize>> {
    let names: Vec<&String> = partial.fields().iter().take(group_columns).map(|f| f.name()).collect();
    if group_names.len() != group_columns {
        return Err(NodraError::job(format!(
            "internal: the aggregate has {group_columns} group columns in the physical plan, {} in the logical plan",
            group_names.len()
        )));
    }
    let mut order = Vec::with_capacity(partial.fields().len());
    for wanted in group_names {
        let mut hits = names.iter().enumerate().filter(|(_, n)| **n == wanted);
        match (hits.next(), hits.next()) {
            (Some((j, _)), None) => order.push(j),
            _ => {
                return Err(NodraError::job(format!(
                    "internal: group column `{wanted}` not found once among the physical group columns {names:?}"
                )))
            }
        }
    }
    order.extend(group_columns..partial.fields().len());
    Ok(order)
}

/// Positional names for partial-state columns (`s0`, `s1`, …): DataFusion's state field names
/// such as `count(x)[count]` are not valid Delta column names.
pub fn state_schema(partial: &SchemaRef) -> SchemaRef {
    let fields: Vec<Field> = partial
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| Field::new(format!("s{i}"), f.data_type().clone(), true))
        .collect();
    Arc::new(Schema::new(fields))
}

/// The plan of the merge step: `plan` with the split aggregate replaced by a `Final` aggregate
/// over `states`. `states` must produce columns named `s0..sN` in the canonical layout (any
/// column order, any types castable to the partial schema); they are mapped, cast and renamed
/// to this plan's partial schema here.
pub fn merge_plan(
    plan: &Arc<dyn ExecutionPlan>,
    states: Arc<dyn ExecutionPlan>,
    group_names: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let split = find_split(plan).ok_or_else(|| {
        NodraError::job("internal: no aggregate over the driving source found in the physical plan")
    })?;
    let agg = split.downcast_ref::<AggregateExec>().expect("aggregate");
    // This plan's partial layout (before canonical reordering).
    let partial_schema = match agg.mode() {
        AggregateMode::Partial => agg.schema(),
        _ => AggregateExec::try_new(
            AggregateMode::Partial,
            agg.group_expr().clone(),
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            agg.input().clone(),
            agg.input_schema(),
        )?
        .schema(),
    };
    let order = canonical_order(&partial_schema, agg.group_expr().num_group_exprs(), group_names)?;
    let restored = restore_states(states, &partial_schema, &order)?;
    // The operators above the split were planned for the split's output partitioning (often a
    // single partition, because the merge is planned over an empty placeholder). The state scan
    // can have any number of partitions, so reproduce that partitioning: either one partition,
    // or a hash partitioning on the group columns (which keeps every group in one partition).
    let partitions = split.output_partitioning().partition_count();
    let group_columns = agg.group_expr().num_group_exprs();
    let fit = |input: Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
        if partitions <= 1 || group_columns == 0 {
            return Ok(coalesce(input));
        }
        let keys: Vec<Arc<dyn PhysicalExpr>> = (0..group_columns)
            .map(|i| Arc::new(Column::new(partial_schema.field(i).name(), i)) as Arc<dyn PhysicalExpr>)
            .collect();
        Ok(Arc::new(RepartitionExec::try_new(input, Partitioning::Hash(keys, partitions))?))
    };
    let final_agg = |mode: AggregateMode, input: Arc<dyn ExecutionPlan>| -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(AggregateExec::try_new(
            mode,
            agg.group_expr().as_final(),
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            input,
            agg.input_schema(),
        )?))
    };
    let replacement: Arc<dyn ExecutionPlan> = match agg.mode() {
        AggregateMode::Partial => fit(restored)?,
        AggregateMode::SinglePartitioned => final_agg(AggregateMode::FinalPartitioned, fit(restored)?)?,
        _ => final_agg(AggregateMode::Final, coalesce(restored))?,
    };
    let replacement = rename_like(replacement, &split.schema())?;
    let target = Arc::as_ptr(&split) as *const () as usize;
    let rewritten = plan.clone().transform_down(|node| {
        if Arc::as_ptr(&node) as *const () as usize == target {
            Ok(Transformed::yes(replacement.clone()))
        } else {
            Ok(Transformed::no(node))
        }
    })?;
    if !rewritten.transformed {
        return Err(NodraError::job("internal: failed to replace the split aggregate"));
    }
    Ok(rewritten.data)
}

fn coalesce(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    if input.output_partitioning().partition_count() == 1 {
        input
    } else {
        Arc::new(CoalescePartitionsExec::new(input))
    }
}

/// Give `plan`'s columns the names of `like` (same types, same order). Optimizers may have
/// rewritten the split aggregate's expressions (e.g. `first_value(x ORDER BY y DESC)` into
/// `last_value(x ORDER BY y)`), and the operators above refer to the original names.
fn rename_like(plan: Arc<dyn ExecutionPlan>, like: &SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
    let have = plan.schema();
    if have.fields().len() != like.fields().len()
        || have.fields().iter().zip(like.fields()).any(|(a, b)| a.data_type() != b.data_type())
    {
        return Err(NodraError::job(format!(
            "internal: merge plan produces {have:?} where the query expects {like:?}"
        )));
    }
    if have.fields().iter().zip(like.fields()).all(|(a, b)| a.name() == b.name()) {
        return Ok(plan);
    }
    let exprs = have
        .fields()
        .iter()
        .zip(like.fields())
        .enumerate()
        .map(|(i, (a, b))| (Arc::new(Column::new(a.name(), i)) as Arc<dyn PhysicalExpr>, b.name().clone()))
        .collect::<Vec<_>>();
    Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
}

/// Project `states` (columns `s0..sN`, canonical layout) to the partial schema: order, types
/// and names. `order[i]` is the partial-schema field stored as `s<i>`.
fn restore_states(states: Arc<dyn ExecutionPlan>, partial: &SchemaRef, order: &[usize]) -> Result<Arc<dyn ExecutionPlan>> {
    let input = states.schema();
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(partial.fields().len());
    for (j, f) in partial.fields().iter().enumerate() {
        let i = order.iter().position(|o| *o == j).expect("order is a permutation");
        let name = format!("s{i}");
        let idx = input
            .index_of(&name)
            .map_err(|_| NodraError::delta(format!("partial-state table is missing column `{name}`")))?;
        let col: Arc<dyn PhysicalExpr> = Arc::new(Column::new(&name, idx));
        let expr: Arc<dyn PhysicalExpr> = if input.field(idx).data_type() == f.data_type() {
            col
        } else {
            Arc::new(CastExpr::new(col, f.data_type().clone(), None))
        };
        exprs.push((expr, f.name().clone()));
    }
    Ok(Arc::new(ProjectionExec::try_new(exprs, states)?))
}
