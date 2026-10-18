//! The `nodra` binary end to end.
mod common;

use std::process::Command;

use common::*;

fn nodra(args: &[&str]) -> (i32, String, String) {
    let out = Command::new(env!("CARGO_BIN_EXE_nodra")).args(args).env("RUST_LOG", "warn").output().unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plan_run_status_merge() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dimp, out) = (dir.path().join("events"), dir.path().join("dim"), dir.path().join("out"));
    events_table(&src, 3, 20).await;
    dim_table(&dimp).await;
    let sql = format!(
        "SET nodra.merge_interval = '0s';\nINSERT INTO delta('{}', partition_by => 'brand')\n\
         SELECT d.brand, count(*) AS n FROM delta('{}') e JOIN delta('{}') d ON e.cid = d.cid GROUP BY d.brand;\n",
        loc(&out),
        loc(&src),
        loc(&dimp)
    );
    let job = dir.path().join("brands.sql");
    std::fs::write(&job, &sql).unwrap();
    let job = loc(&job);

    let (code, stdout, stderr) = nodra(&["plan", &job]);
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.contains("class:   3 · mergeable aggregation"), "{stdout}");
    assert!(stdout.contains("broadcast"), "{stdout}");

    let (code, _, stderr) = nodra(&["run", &job, "--once", "--local-workers", "3"]);
    assert_eq!(code, 0, "{stderr}");
    assert_matches_reference(&sql, &[]).await;

    let (code, stdout, stderr) = nodra(&["status", &job]);
    assert_eq!(code, 0, "{stderr}");
    assert!(stdout.contains("workers: 3"), "{stdout}");
    assert!(stdout.contains("merge:"), "{stdout}");

    // Distributed style: workers as separate processes, merge as its own step.
    append(&src, events(60..90)).await;
    for i in 0..3 {
        let (code, _, stderr) = nodra(&["run", &job, "--once", "--worker", &i.to_string(), "--workers", "3"]);
        assert_eq!(code, 0, "{stderr}");
    }
    let (code, _, stderr) = nodra(&["merge", &job, "--once"]);
    assert_eq!(code, 0, "{stderr}");
    assert_matches_reference(&sql, &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_job_and_usage_errors() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("events");
    events_table(&src, 1, 5).await;
    let job = dir.path().join("bad.sql");
    std::fs::write(&job, format!("INSERT INTO delta('/tmp/x') SELECT id FROM delta('{}') LIMIT 3", loc(&src))).unwrap();
    let (code, _, stderr) = nodra(&["plan", &loc(&job)]);
    assert_eq!(code, 1);
    assert!(stderr.contains("plan rejected: LIMIT"), "{stderr}");

    let (code, _, stderr) = nodra(&["run"]);
    assert_eq!(code, 2, "{stderr}");
    assert!(stderr.contains("USAGE"));
    let (code, stdout, _) = nodra(&["version"]);
    assert_eq!(code, 0);
    assert!(stdout.starts_with("nodra "));
}
