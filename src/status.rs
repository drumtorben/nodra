//! `nodra status`: progress of a job, read from its target tables.

use std::fmt::Write as _;

use crate::delta;
use crate::error::Result;
use crate::exec::{partials_location, Pipeline};
use crate::job::SourceSpec;
use crate::planner::JobClass;
use crate::source::jsonl::JsonlStream;
use crate::source::stream::StreamRef;

/// Render the progress of `p`. `workers`: worker count to look up for Delta-driven jobs
/// (default: probe 1..=64).
pub async fn render(p: &Pipeline, workers: Option<u32>) -> Result<String> {
    let mut s = String::new();
    let target_loc = p.target_location();
    let _ = writeln!(s, "job:     {} (class {})", p.job.name, p.plan.class.number());
    let Some(target) = delta::try_open(&target_loc).await? else {
        let _ = writeln!(s, "target:  {target_loc} does not exist yet (no batch committed)");
        return Ok(s);
    };
    let _ = writeln!(s, "target:  {target_loc} @ version {}", delta::version(&target)?);
    if let Some(si) = p.job.stream_source() {
        let src: Option<StreamRef> = match &p.job.sources[si] {
            SourceSpec::JsonlStream { dir, .. } => JsonlStream::new(dir).ok().map(|j| std::sync::Arc::new(j) as StreamRef),
            #[cfg(feature = "kafka")]
            SourceSpec::Kafka { topic, brokers, properties, .. } => {
                crate::source::kafka::KafkaSource::new(topic, brokers, &format!("nodra-{}", p.job.name), properties)
                    .ok()
                    .map(|k| std::sync::Arc::new(k) as StreamRef)
            }
            _ => None,
        };
        let _ = writeln!(s, "partitions:");
        if let Some(src) = src {
            for part in src.partitions().await? {
                let committed = delta::txn_version(&target, &p.stream_progress_key(si, part)).await?;
                let (_, high) = src.watermarks(part).await?;
                let lag = committed.map(|c| (high - c).max(0)).map(|l| l.to_string()).unwrap_or_else(|| "-".into());
                let _ = writeln!(
                    s,
                    "  p{part}: next offset {}, high watermark {high}, lag {lag}",
                    committed.map(|c| c.to_string()).unwrap_or_else(|| "none".into())
                );
            }
        }
    } else if p.plan.class != JobClass::CoPartitionedJoin {
        let di = p.plan.driving()[0];
        match delta::txn_map(&target).await? {
            Some(all) => {
                let gens = crate::rescale::generations(&all, &p.delta_source_prefix(di))?;
                let latest = gens.keys().next_back().copied();
                for (g, gen) in &gens {
                    let state = if Some(*g) == latest { "current" } else { "previous run" };
                    let _ = writeln!(s, "workers: {} (generation {g}, {state})", gen.count);
                    for i in 0..gen.count {
                        let v = match (gen.done.get(&i), gen.snapshot.contains(&i)) {
                            (Some(v), _) => format!("source version {v}"),
                            (None, true) => "processing the initial snapshot".to_string(),
                            (None, false) => "no progress yet".to_string(),
                        };
                        let _ = writeln!(s, "  worker {i}: {v}");
                    }
                }
            }
            None => {
                // The keys cannot be listed (v2 checkpoint): probe worker counts.
                let range: Vec<u32> = match workers {
                    Some(n) => vec![n],
                    None => (1..=64).collect(),
                };
                for n in range {
                    let mut found = Vec::new();
                    for i in 0..n {
                        if let Some(v) = delta::txn_version(&target, &p.delta_progress_key(di, i, n)).await? {
                            found.push((i, v));
                        }
                    }
                    if !found.is_empty() {
                        let _ = writeln!(s, "workers: {n}");
                        for (i, v) in found {
                            let _ = writeln!(s, "  worker {i}: source version {v}");
                        }
                    }
                }
            }
        }
    } else {
        let parts = delta::txn_map(&target).await?.map(|all| {
            let prefix = format!("nodra/{}/part/", p.job.name);
            all.keys().filter(|k| k.starts_with(&prefix)).count()
        });
        if let Some(n) = parts {
            let _ = writeln!(s, "partitions: {n} processed");
        }
    }
    if p.plan.class == JobClass::MergeableAggregation {
        let partials = delta::try_open(&partials_location(&p.job)).await?;
        let sink = delta::try_open(p.job.sink.uri()).await?;
        let merged = match &sink {
            Some(t) => delta::txn_version(t, &p.merge_progress_key()).await?,
            None => None,
        };
        let latest = match &partials {
            Some(t) => Some(delta::version(t)?),
            None => None,
        };
        let _ = writeln!(
            s,
            "merge:   merged partials version {} of {}",
            merged.map(|v| v.to_string()).unwrap_or_else(|| "none".into()),
            latest.map(|v| v.to_string()).unwrap_or_else(|| "none".into())
        );
    }
    Ok(s)
}
