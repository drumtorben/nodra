//! DataFusion session setup.

use datafusion::execution::config::SessionConfig;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::physical_optimizer::optimizer::PhysicalOptimizer;
use datafusion::prelude::SessionContext;

use crate::error::{NodraError, Result};
use crate::settings::Settings;

/// A DataFusion context configured from the job's engine settings
/// (`SET datafusion.… = …`).
///
/// Two optimizer options are fixed (see the comments below), and the `aggregate_statistics`
/// physical optimizer rule is disabled: it answers aggregates such
/// as `count(*)` from table statistics, which is wrong for Nodra because a worker only ever sees
/// a slice of the input, and it would remove the aggregate the merge step relies on.
pub fn new_context(settings: &Settings) -> Result<SessionContext> {
    let mut config = SessionConfig::new().with_information_schema(false);
    for (k, v) in settings.engine_options() {
        config
            .options_mut()
            .set(k, v)
            .map_err(|e| NodraError::config(format!("invalid setting `{k} = {v}`: {e}")))?;
    }
    let opts = config.options_mut();
    // Uncorrelated scalar subqueries become joins (not a wrapper at the plan root, which the
    // class-3 worker plan would cut off).
    opts.optimizer.enable_physical_uncorrelated_scalar_subquery = false;
    // Keep aggregate group keys in their logical order where possible (see `partial`).
    opts.optimizer.top_down_join_key_reordering = false;
    let rules =
        PhysicalOptimizer::new().rules.into_iter().filter(|r| r.name() != "aggregate_statistics").collect();
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rules(rules)
        .build();
    Ok(SessionContext::new_with_state(state))
}
