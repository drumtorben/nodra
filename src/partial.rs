//! Splitting a physical plan at the aggregation over the driving source (class 3).
//!
//! The driving source is registered with the schema-metadata marker [`DRIVING_MARKER`], so its
//! scan can be found in the physical plan after DataFusion's optimizers ran. The aggregate
//! closest to that scan is the *split aggregate*:
//!
//! * workers run it in `Partial` mode and persist its output (accumulator states),
//! * the merge step replaces it with a `Final` aggregate over a scan of those states and runs
//!   the rest of the plan on top.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::physical_expr::expressions::{CastExpr, Column};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::ExecutionPlan;

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

/// The plan a worker runs for a class-3 job: the split aggregate in `Partial` mode.
pub fn worker_plan(plan: &Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
    let split = find_split(plan).ok_or_else(|| {
        NodraError::job("internal: no aggregate over the driving source found in the physical plan")
    })?;
    let agg = split.downcast_ref::<AggregateExec>().expect("aggregate");
    match agg.mode() {
        AggregateMode::Partial => Ok(split.clone()),
        AggregateMode::Single | AggregateMode::SinglePartitioned => Ok(Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            agg.group_expr().clone(),
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            agg.input().clone(),
            agg.input_schema(),
        )?)),
        other => Err(NodraError::job(format!(
            "internal: unexpected aggregate mode {other:?} next to the driving source"
        ))),
    }
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
/// over `states`. `states` must produce columns named `s0..sN` (any order, any types castable to
/// the partial schema); they are reordered, cast and renamed to the partial schema here.
pub fn merge_plan(
    plan: &Arc<dyn ExecutionPlan>,
    states: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let split = find_split(plan).ok_or_else(|| {
        NodraError::job("internal: no aggregate over the driving source found in the physical plan")
    })?;
    let agg = split.downcast_ref::<AggregateExec>().expect("aggregate");
    let partial_schema = match agg.mode() {
        AggregateMode::Partial => agg.schema(),
        _ => worker_plan(plan)?.schema(),
    };
    let restored = restore_states(states, &partial_schema)?;
    let replacement: Arc<dyn ExecutionPlan> = match agg.mode() {
        AggregateMode::Partial => restored,
        _ => Arc::new(AggregateExec::try_new(
            AggregateMode::Final,
            agg.group_expr().as_final(),
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            Arc::new(CoalescePartitionsExec::new(restored)),
            agg.input_schema(),
        )?),
    };
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

/// Project `states` (columns `s0..sN`) to the partial schema: order, types and names.
fn restore_states(states: Arc<dyn ExecutionPlan>, partial: &SchemaRef) -> Result<Arc<dyn ExecutionPlan>> {
    let input = states.schema();
    let mut exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = Vec::with_capacity(partial.fields().len());
    for (i, f) in partial.fields().iter().enumerate() {
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
