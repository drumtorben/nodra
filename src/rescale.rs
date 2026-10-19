//! Worker generations of Delta-driven jobs: changing the worker count without losing or
//! repeating input (see `docs/design.md`, "Rescaling").
//!
//! Files of a Delta driving source are assigned with `fnv(path) mod n`, and each worker's
//! progress is the last source version it fully processed. The workers of one *generation*
//! share a worker count. Generation 0 uses the keys `<prefix>w<i>of<n>`, generation `g > 0`
//! the keys `<prefix>g<g>/w<i>of<n>`.
//!
//! Starting the job with a different worker count starts a new generation. Its workers begin at
//! the smallest progress of the previous generation and skip every file that an earlier
//! generation already processed: file `f` added in version `v` was processed by generation `g`
//! (with `m` workers and final progress `done`) iff `v <= done[fnv(f) mod m]`. The handover
//! needs the final progress of the earlier generations, so their workers must have stopped; the
//! first commit of the new generation also rewrites the previous generation's progress keys,
//! which makes a still-running old worker's next commit fail Delta's conflict check, and old
//! workers stop once they see a newer generation.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::error::{NodraError, Result};
use crate::hash::worker_for;

/// Progress key of worker `i` of `n` in generation `generation`.
pub fn worker_key(prefix: &str, generation: u32, i: u32, n: u32) -> String {
    if generation == 0 {
        format!("{prefix}w{i}of{n}")
    } else {
        format!("{prefix}g{generation}/w{i}of{n}")
    }
}

/// The source version all workers of the first generation start from.
pub fn start_key(prefix: &str) -> String {
    format!("{prefix}start")
}

/// The high watermark of stream partition `partition` when the job started (`start = latest`).
pub fn stream_start_key(prefix: &str, partition: i32) -> String {
    format!("{prefix}start/p{partition}")
}

/// All recorded stream start offsets (see [`stream_start_key`]), by partition.
pub fn stream_starts(all: Option<&HashMap<String, i64>>, prefix: &str) -> HashMap<i32, i64> {
    let start = format!("{prefix}start/p");
    all.into_iter()
        .flatten()
        .filter_map(|(k, v)| Some((k.strip_prefix(&start)?.parse::<i32>().ok()?, *v)))
        .collect()
}

/// Snapshot-chunk key that belongs to a worker key.
pub fn snapshot_key(worker_key: &str) -> String {
    format!("{worker_key}/snapshot")
}

/// The progress of one generation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Generation {
    /// Worker count.
    pub count: u32,
    /// Last fully processed source version, by worker index.
    pub done: BTreeMap<u32, i64>,
    /// Workers with initial-snapshot progress.
    pub snapshot: BTreeSet<u32>,
}

/// Parse all generations from the `txn` versions of the target table. `prefix` is
/// `nodra/<job>/<source key>/`; other keys are ignored.
pub fn generations(all: &HashMap<String, i64>, prefix: &str) -> Result<BTreeMap<u32, Generation>> {
    let mut out: BTreeMap<u32, Generation> = BTreeMap::new();
    for (key, version) in all {
        let Some(rest) = key.strip_prefix(prefix) else { continue };
        let (generation, rest) = match rest.strip_prefix('g') {
            Some(r) => match r.split_once('/') {
                Some((g, r)) => match g.parse::<u32>() {
                    Ok(g) if g > 0 => (g, r),
                    _ => continue,
                },
                None => continue,
            },
            None => (0, rest),
        };
        let Some(rest) = rest.strip_prefix('w') else { continue };
        let (worker, suffix) = match rest.split_once('/') {
            Some((w, s)) => (w, Some(s)),
            None => (rest, None),
        };
        let Some((i, n)) = worker.split_once("of") else { continue };
        let (Ok(i), Ok(n)) = (i.parse::<u32>(), n.parse::<u32>()) else { continue };
        let g = out.entry(generation).or_insert_with(|| Generation { count: n, ..Default::default() });
        if g.count != n {
            return Err(NodraError::delta(format!(
                "progress keys of generation {generation} disagree on the worker count ({} and {n})",
                g.count
            )));
        }
        match suffix {
            None => {
                g.done.insert(i, *version);
            }
            Some("snapshot") => {
                g.snapshot.insert(i);
            }
            Some(_) => {}
        }
    }
    Ok(out)
}

/// Where a worker of a given count belongs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    /// The worker's generation.
    pub generation: u32,
    /// The previous generation's final progress, if this generation took over from one.
    pub handover: Option<Handover>,
}

/// The final progress of the previous generation (and of the ones before it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handover {
    /// The previous generation.
    pub generation: u32,
    /// Its worker count.
    pub count: u32,
    /// Last processed source version of each of its workers.
    pub done: Vec<i64>,
    /// Worker count and final progress of every generation before the previous one.
    pub earlier: Vec<(u32, Vec<i64>)>,
}

impl Handover {
    /// The version the new generation starts after.
    pub fn start(&self) -> i64 {
        self.done.iter().copied().min().unwrap_or(-1)
    }

    /// True if the file at `path`, added in source version `version`, was already processed by
    /// an earlier generation.
    pub fn processed(&self, path: &str, version: i64) -> bool {
        version <= self.done[worker_for(path, self.count) as usize]
            || self.earlier.iter().any(|(n, done)| version <= done[worker_for(path, *n) as usize])
    }

    /// True while the handover can still skip files (versions up to the largest old progress).
    pub fn active(&self, version: i64) -> bool {
        version <= self.done.iter().copied().max().unwrap_or(-1)
    }
}

/// Place a worker of `count` workers, given the generations found in the target.
pub fn place(gens: &BTreeMap<u32, Generation>, count: u32) -> Result<Placement> {
    let Some((&latest, last)) = gens.iter().next_back() else {
        return Ok(Placement { generation: 0, handover: None });
    };
    let (generation, previous) =
        if last.count == count { (latest, latest.checked_sub(1)) } else { (latest + 1, Some(latest)) };
    let handover = match previous {
        None => None,
        Some(prev) => {
            let g = gens.get(&prev).ok_or_else(|| {
                NodraError::delta(format!(
                    "progress of worker generation {prev} is missing from the target table"
                ))
            })?;
            let mut done = Vec::with_capacity(g.count as usize);
            for j in 0..g.count {
                match g.done.get(&j) {
                    Some(v) => done.push(*v),
                    None => {
                        let why = if g.snapshot.contains(&j) {
                            "is still processing the initial snapshot"
                        } else {
                            "never committed progress"
                        };
                        return Err(NodraError::config(format!(
                            "cannot change the worker count from {} to {count} yet: worker {j} of the previous run {why}; \
                             run the job with {} workers until `nodra status` shows a source version for every worker",
                            g.count, g.count
                        )));
                    }
                }
            }
            // Earlier generations were complete when their successors took over.
            let earlier = gens
                .range(..prev)
                .map(|(_, e)| {
                    (e.count, (0..e.count).map(|j| e.done.get(&j).copied().unwrap_or(-1)).collect())
                })
                .collect();
            Some(Handover { generation: prev, count: g.count, done, earlier })
        }
    };
    Ok(Placement { generation, handover })
}

#[cfg(test)]
mod tests {
    use super::*;

    const P: &str = "nodra/j/delta:/t/";

    fn all(keys: &[(&str, i64)]) -> HashMap<String, i64> {
        keys.iter().map(|(k, v)| (format!("{P}{k}"), *v)).collect()
    }

    #[test]
    fn keys_roundtrip() {
        assert_eq!(worker_key(P, 0, 1, 4), format!("{P}w1of4"));
        assert_eq!(worker_key(P, 3, 0, 2), format!("{P}g3/w0of2"));
        let m = all(&[("w0of2", 10), ("w1of2", 12), ("w1of2/snapshot", 5), ("g1/w0of3", 20), ("p7", 99)]);
        let mut extra = m.clone();
        extra.insert("nodra/other/delta:/t/w0of9".into(), 1);
        let gens = generations(&extra, P).unwrap();
        assert_eq!(gens.len(), 2);
        assert_eq!(gens[&0].count, 2);
        assert_eq!(gens[&0].done, BTreeMap::from([(0, 10), (1, 12)]));
        assert_eq!(gens[&0].snapshot, BTreeSet::from([1]));
        assert_eq!(gens[&1].count, 3);
        assert!(generations(&all(&[("w0of2", 1), ("w1of3", 1)]), P).is_err());
    }

    #[test]
    fn placement() {
        // New job.
        assert_eq!(place(&BTreeMap::new(), 4).unwrap(), Placement { generation: 0, handover: None });
        // Same count: same generation.
        let g0 = generations(&all(&[("w0of2", 10), ("w1of2", 12)]), P).unwrap();
        assert_eq!(place(&g0, 2).unwrap().generation, 0);
        // New count: next generation, taking over from generation 0.
        let p = place(&g0, 3).unwrap();
        assert_eq!(p.generation, 1);
        let h = p.handover.unwrap();
        assert_eq!((h.generation, h.count, h.start()), (0, 2, 10));
        // Restarted worker of generation 1 still knows the handover.
        let g01 = generations(&all(&[("w0of2", 10), ("w1of2", 12), ("g1/w2of3", 15)]), P).unwrap();
        assert_eq!(place(&g01, 3).unwrap().handover.unwrap().done, vec![10, 12]);
        // Back to 2 workers: generation 2, taking over from generation 1.
        let g = generations(
            &all(&[("w0of2", 10), ("w1of2", 12), ("g1/w0of3", 20), ("g1/w1of3", 21), ("g1/w2of3", 22)]),
            P,
        )
        .unwrap();
        let p = place(&g, 2).unwrap();
        assert_eq!(p.generation, 2);
        let h = p.handover.unwrap();
        assert_eq!(h.done, vec![20, 21, 22]);
        assert_eq!(h.earlier, vec![(2, vec![10, 12])]);
        // The previous generation must have progress for every worker.
        let g = generations(&all(&[("w0of2", 10), ("w1of2/snapshot", 3)]), P).unwrap();
        assert!(place(&g, 3).unwrap_err().to_string().contains("initial snapshot"));
        let g = generations(&all(&[("w0of2", 10)]), P).unwrap();
        assert!(place(&g, 3).unwrap_err().to_string().contains("never committed"));
    }

    #[test]
    fn handover_skips_files_of_the_previous_owner() {
        let h = Handover { generation: 0, count: 2, done: vec![10, 20], earlier: vec![] };
        let (a, b) = ((0..).map(|i| format!("f{i}")).find(|f| worker_for(f, 2) == 0).unwrap(), {
            (0..).map(|i| format!("f{i}")).find(|f| worker_for(f, 2) == 1).unwrap()
        });
        assert!(h.processed(&a, 10) && !h.processed(&a, 11));
        assert!(h.processed(&b, 20) && !h.processed(&b, 21));
        assert_eq!(h.start(), 10);
        assert!(h.active(20) && !h.active(21));
    }
}
