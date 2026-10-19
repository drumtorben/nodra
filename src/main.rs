//! `nodra` command-line entry point.

use std::process::ExitCode;
use std::sync::Arc;

use nodra::cli::{self, Command, CommonArgs, RunArgs, WorkerSpec};
use nodra::exec::{self, Pipeline};
use nodra::job::Job;
use nodra::{merge, status, NodraError, Result};
use tokio::sync::watch;
use tracing::{error, info, Level};

/// Logging: `RUST_LOG` sets Nodra's level (`error|warn|info|debug|trace`, default `info`);
/// libraries log at `warn` unless `RUST_LOG=debug,all` / `trace,all` is given.
fn init_logging() {
    use std::io::IsTerminal;
    use tracing_subscriber::filter::Targets;
    use tracing_subscriber::prelude::*;
    let spec = std::env::var("RUST_LOG").unwrap_or_default().to_ascii_lowercase();
    let mut parts = spec.split(',').map(str::trim);
    let level = match parts.next().unwrap_or("") {
        "trace" => Level::TRACE,
        "debug" => Level::DEBUG,
        "warn" => Level::WARN,
        "error" => Level::ERROR,
        _ => Level::INFO,
    };
    let libs = if parts.any(|p| p == "all") { level } else { Level::WARN.min(level) };
    // delta-rs warns about every commit that had to be retried after a concurrent commit, which
    // is the normal case for Nodra's workers.
    // Same for statistics skipped on binary columns (partial aggregate states).
    let quiet = if libs > Level::WARN { libs } else { Level::ERROR };
    // delta-kernel logs every error it returns (`#[instrument(err)]`); Nodra reports them itself.
    let kernel = if libs > Level::WARN {
        tracing_subscriber::filter::LevelFilter::from_level(libs)
    } else {
        tracing_subscriber::filter::LevelFilter::OFF
    };
    let filter = Targets::new()
        .with_target("nodra", level)
        .with_target("deltalake_core::kernel::transaction", quiet)
        .with_target("deltalake_core::writer::stats", quiet)
        .with_target("buoyant_kernel", kernel)
        .with_default(libs);
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_writer(std::io::stderr)
                .with_ansi(std::io::stderr().is_terminal()),
        )
        .with(filter)
        .init();
}

#[tokio::main]
async fn main() -> ExitCode {
    init_logging();
    let args: Vec<String> = std::env::args().skip(1).collect();
    match real_main(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(NodraError::Config(msg)) if msg.contains("USAGE") => {
            eprintln!("{msg}");
            ExitCode::from(2)
        }
        Err(e) => {
            // One report: plain text on a terminal, a log line for log collectors otherwise.
            if std::io::IsTerminal::is_terminal(&std::io::stderr()) {
                eprintln!("error: {e}");
            } else {
                error!("{e}");
            }
            ExitCode::FAILURE
        }
    }
}

async fn real_main(args: Vec<String>) -> Result<()> {
    match cli::parse(&args)? {
        Command::Help => {
            print!("{}", cli::USAGE);
            Ok(())
        }
        Command::Version => {
            println!("nodra {}", env!("CARGO_PKG_VERSION"));
            Ok(())
        }
        Command::Plan(common) => {
            let p = prepare(&common).await?;
            print!("{}", p.explain());
            Ok(())
        }
        Command::Status(common) => {
            let p = prepare(&common).await?;
            print!("{}", status::render(&p, None).await?);
            Ok(())
        }
        Command::Run(run) => run_cmd(run).await,
        Command::Merge(run) => merge_cmd(run).await,
    }
}

fn load_job(common: &CommonArgs) -> Result<Job> {
    let mut job = Job::from_file(&common.job)?;
    for (k, v) in &common.overrides {
        job.settings.set(k, v)?;
    }
    if let Some(name) = common.overrides.iter().find(|(k, _)| k == "nodra.job_name").map(|(_, v)| v.clone()) {
        job.name = name;
    }
    Ok(job)
}

async fn prepare(common: &CommonArgs) -> Result<Pipeline> {
    Pipeline::prepare(load_job(common)?).await
}

fn env_u32(name: &str) -> Result<Option<u32>> {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v
            .parse()
            .map(Some)
            .map_err(|_| NodraError::config(format!("{name} must be a non-negative integer, got `{v}`"))),
        _ => Ok(None),
    }
}

/// Resolve `(local worker indices, worker count)`.
fn workers(run: &RunArgs) -> Result<(Vec<u32>, u32)> {
    if let Some(n) = run.local_workers {
        if n == 0 {
            return Err(NodraError::config("--local-workers must be at least 1"));
        }
        return Ok(((0..n).collect(), n));
    }
    let count = run.workers.or(env_u32("NODRA_WORKERS")?).unwrap_or(1);
    let index = match &run.worker {
        Some(WorkerSpec::Index(i)) => *i,
        Some(WorkerSpec::Auto) => {
            let host = std::env::var("HOSTNAME")
                .or_else(|_| std::fs::read_to_string("/etc/hostname").map(|s| s.trim().to_string()))
                .unwrap_or_default();
            cli::ordinal_from_hostname(&host).ok_or_else(|| {
                NodraError::config(format!("--worker auto: cannot derive an ordinal from host name `{host}`"))
            })?
        }
        None => env_u32("NODRA_WORKER")?.unwrap_or(0),
    };
    if index >= count {
        return Err(NodraError::config(format!("worker index {index} is not below the worker count {count}")));
    }
    Ok((vec![index], count))
}

fn stop_signal() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    tokio::spawn(async move {
        let term = async {
            #[cfg(unix)]
            {
                if let Ok(mut s) = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                    s.recv().await;
                    return;
                }
            }
            std::future::pending::<()>().await;
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term => {}
        }
        info!("stopping after the current batch (signal again to abort)");
        let _ = tx.send(true);
        let _ = tokio::signal::ctrl_c().await;
        std::process::exit(130);
    });
    rx
}

async fn run_cmd(run: RunArgs) -> Result<()> {
    let (indices, count) = workers(&run)?;
    let p = Arc::new(prepare(&run.common).await?);
    if let Some(addr) = run.metrics_addr.clone() {
        tokio::spawn(async move {
            if let Err(e) = nodra::metrics::serve(addr).await {
                error!("metrics endpoint failed: {e}");
            }
        });
    }
    exec::run(p, indices, count, run.once, stop_signal()).await
}

async fn merge_cmd(run: RunArgs) -> Result<()> {
    let p = prepare(&run.common).await?;
    if p.plan.class != nodra::planner::JobClass::MergeableAggregation {
        return Err(NodraError::config(format!(
            "`{}` is a class-{} job and has no merge step",
            p.job.name,
            p.plan.class.number()
        )));
    }
    let interval = p.job.settings.merge_interval()?.max(std::time::Duration::from_secs(1));
    let mut stop = stop_signal();
    loop {
        let outcome = merge::merge(&p).await?;
        info!(?outcome, "merge");
        if run.once || *stop.borrow() {
            return Ok(());
        }
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = stop.changed() => return Ok(()),
        }
    }
}
