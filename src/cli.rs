//! Command-line parsing (kept dependency-free on purpose).

use std::path::PathBuf;

use crate::error::{NodraError, Result};

/// Top-level command.
#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    /// `nodra run <job.sql>`
    Run(RunArgs),
    /// `nodra plan <job.sql> [--explain]`
    Plan(CommonArgs),
    /// `nodra merge <job.sql> [--once]`
    Merge(RunArgs),
    /// `nodra status <job.sql>`
    Status(CommonArgs),
    /// `nodra version`
    Version,
    /// `nodra help`
    Help,
}

/// Arguments shared by all job commands.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct CommonArgs {
    /// Path to the job file.
    pub job: PathBuf,
    /// `--set key=value` overrides, applied after the job's own `SET` statements.
    pub overrides: Vec<(String, String)>,
}

/// Arguments of `run` and `merge`.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RunArgs {
    /// Job file and overrides.
    pub common: CommonArgs,
    /// Which worker this process is (`--worker`), resolved from `auto` if requested.
    pub worker: Option<WorkerSpec>,
    /// Total number of workers (`--workers`).
    pub workers: Option<u32>,
    /// Run `n` workers inside this process (`--local-workers n`), mainly for tests and demos.
    pub local_workers: Option<u32>,
    /// Process the currently available input, then exit (`--once`).
    pub once: bool,
    /// Serve Prometheus metrics on this address (`--metrics-addr 0.0.0.0:9464`).
    pub metrics_addr: Option<String>,
}

/// How the worker index is given.
#[derive(Debug, Clone, PartialEq)]
pub enum WorkerSpec {
    /// Explicit index.
    Index(u32),
    /// Derive from the host name's trailing `-<n>` (Kubernetes StatefulSet ordinal).
    Auto,
}

/// Usage text.
pub const USAGE: &str = "\
nodra — driverless, shuffle-free SQL pipelines on DataFusion and Delta Lake

USAGE:
    nodra run <job.sql> [--worker <i|auto>] [--workers <n>] [--once] [--set k=v]...
                        [--local-workers <n>] [--metrics-addr <host:port>]
    nodra plan <job.sql> [--explain] [--set k=v]...
    nodra merge <job.sql> [--once] [--set k=v]...
    nodra status <job.sql> [--workers <n>] [--set k=v]...
    nodra version

ENVIRONMENT:
    NODRA_WORKER, NODRA_WORKERS   defaults for --worker / --workers
    RUST_LOG                      log filter (default: info)
";

/// Parse `args` (without the program name).
pub fn parse(args: &[String]) -> Result<Command> {
    let mut it = args.iter();
    let cmd = match it.next() {
        None => return Ok(Command::Help),
        Some(c) => c.as_str(),
    };
    match cmd {
        "help" | "-h" | "--help" => return Ok(Command::Help),
        "version" | "-V" | "--version" => return Ok(Command::Version),
        "run" | "plan" | "merge" | "status" => {}
        other => return Err(NodraError::config(format!("unknown command `{other}`\n\n{USAGE}"))),
    }
    let mut run = RunArgs::default();
    let mut job: Option<PathBuf> = None;
    let rest: Vec<&String> = it.collect();
    let mut i = 0;
    let value = |i: &mut usize, flag: &str| -> Result<String> {
        *i += 1;
        rest.get(*i)
            .map(|s| s.to_string())
            .ok_or_else(|| NodraError::config(format!("{flag} needs a value")))
    };
    while i < rest.len() {
        let a = rest[i].as_str();
        let (flag, inline) = match a.split_once('=') {
            Some((f, v)) if f.starts_with("--") && f != "--set" => (f, Some(v.to_string())),
            _ => (a, None),
        };
        let mut take = |name: &str| -> Result<String> {
            match &inline {
                Some(v) => Ok(v.clone()),
                None => value(&mut i, name),
            }
        };
        match flag {
            "--worker" => {
                let v = take("--worker")?;
                run.worker = Some(if v == "auto" {
                    WorkerSpec::Auto
                } else {
                    WorkerSpec::Index(parse_u32(&v, "--worker")?)
                });
            }
            "--workers" => run.workers = Some(parse_u32(&take("--workers")?, "--workers")?),
            "--local-workers" => {
                run.local_workers = Some(parse_u32(&take("--local-workers")?, "--local-workers")?)
            }
            "--metrics-addr" => run.metrics_addr = Some(take("--metrics-addr")?),
            "--once" => run.once = true,
            "--explain" => {}
            "--set" => {
                let kv = value(&mut i, "--set")?;
                let (k, v) = kv
                    .split_once('=')
                    .ok_or_else(|| NodraError::config(format!("--set expects key=value, got `{kv}`")))?;
                run.common.overrides.push((k.trim().to_string(), v.trim().to_string()));
            }
            f if f.starts_with('-') => {
                return Err(NodraError::config(format!("unknown flag `{f}`\n\n{USAGE}")))
            }
            path => {
                if job.replace(PathBuf::from(path)).is_some() {
                    return Err(NodraError::config("only one job file can be given"));
                }
            }
        }
        i += 1;
    }
    run.common.job = job.ok_or_else(|| NodraError::config(format!("missing job file\n\n{USAGE}")))?;
    Ok(match cmd {
        "run" => Command::Run(run),
        "merge" => Command::Merge(run),
        "plan" => Command::Plan(run.common),
        _ => Command::Status(run.common),
    })
}

fn parse_u32(v: &str, flag: &str) -> Result<u32> {
    v.parse().map_err(|_| NodraError::config(format!("{flag} expects a non-negative integer, got `{v}`")))
}

/// Resolve `--worker auto`: the trailing `-<n>` of the host name (StatefulSet pods are named
/// `<set>-<ordinal>`).
pub fn ordinal_from_hostname(host: &str) -> Option<u32> {
    host.rsplit_once('-').and_then(|(_, n)| n.parse().ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn p(s: &str) -> Result<Command> {
        parse(&s.split_whitespace().map(String::from).collect::<Vec<_>>())
    }

    #[test]
    fn run_flags() {
        let c = p("run job.sql --worker 2 --workers=4 --once --set nodra.trigger=5s").unwrap();
        match c {
            Command::Run(r) => {
                assert_eq!(r.common.job, PathBuf::from("job.sql"));
                assert_eq!(r.worker, Some(WorkerSpec::Index(2)));
                assert_eq!(r.workers, Some(4));
                assert!(r.once);
                assert_eq!(r.common.overrides, vec![("nodra.trigger".into(), "5s".into())]);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn other_commands() {
        assert!(matches!(p("plan j.sql --explain").unwrap(), Command::Plan(_)));
        assert!(matches!(p("status j.sql").unwrap(), Command::Status(_)));
        assert!(matches!(p("merge j.sql --once").unwrap(), Command::Merge(_)));
        assert_eq!(p("version").unwrap(), Command::Version);
        assert_eq!(p("").unwrap(), Command::Help);
        assert!(p("run").is_err());
        assert!(p("run a.sql b.sql").is_err());
        assert!(p("run a.sql --bogus").is_err());
        assert!(p("frobnicate").is_err());
        assert!(matches!(
            p("run a.sql --worker auto").unwrap(),
            Command::Run(RunArgs { worker: Some(WorkerSpec::Auto), .. })
        ));
    }

    #[test]
    fn hostname_ordinal() {
        assert_eq!(ordinal_from_hostname("nodra-hourly-3"), Some(3));
        assert_eq!(ordinal_from_hostname("nodra"), None);
    }
}
