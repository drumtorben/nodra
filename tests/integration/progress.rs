//! Progress tracking of Delta driving sources, and progress lookup after checkpoints.
use crate::common;

use std::sync::Arc;

use common::*;
use deltalake_core::kernel::engine::arrow_conversion::TryIntoKernel;
use deltalake_core::kernel::{StructType, Transaction};
use deltalake_core::operations::create::CreateBuilder;
use nodra::delta::{self, CommitOutcome};

/// The initial-snapshot progress (`.../snapshot` txn) counts processed files, so changing
/// `nodra.max_files_per_batch` between a crashed and a resumed run neither skips nor repeats
/// files.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn changing_max_files_during_initial_snapshot_keeps_files() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 3, 30).await; // 9 files (3 commits x 3 days)
    let mut files = delta::active_files(&delta::open(&loc(&src)).await.unwrap()).unwrap();
    files.sort_by(|a, b| a.path.as_ref().cmp(b.path.as_ref()));
    assert_eq!(files.len(), 9);
    let job = |max: usize| {
        format!(
            "SET nodra.job_name = 'chunks'; SET nodra.max_files_per_batch = '{max}'; \
             INSERT INTO delta('{}') SELECT id, v FROM delta('{}')",
            loc(&out),
            loc(&src)
        )
    };
    // Run 1 (2 files per batch): chunk 0 = files[0..2] commits, chunk 1 fails because files[2]
    // is temporarily unreadable (any transient error / crash at this point does the same).
    let hidden = src.join(files[2].path.as_ref());
    let aside = dir.path().join("aside.parquet");
    std::fs::rename(&hidden, &aside).unwrap();
    assert!(run_once(&job(2), 1).await.is_err());
    std::fs::rename(&aside, &hidden).unwrap();
    // Run 2 with 3 files per batch resumes after the 2 files done.
    run_once(&job(3), 1).await.unwrap();
    assert_matches_reference(&job(3), &[]).await;
}

/// A batch that finds no files for this worker in the next 1000 source commits commits its
/// progress only; a `--once` run must continue with the newer versions after that.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn once_run_continues_after_progress_only_commit() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 1, 10).await;
    let sql = format!("INSERT INTO delta('{}') SELECT id FROM delta('{}')", loc(&out), loc(&src));
    run_once(&sql, 1).await.unwrap();
    // 1100 source commits without data files for anyone (e.g. other writers' txn-only commits).
    let mut t = delta::open(&loc(&src)).await.unwrap();
    let parts = vec!["day".to_string()];
    for i in 0..1100 {
        match delta::commit(
            &mut t,
            vec![],
            vec![Transaction::new("other-app", i)],
            delta::append_op(&parts),
            3,
        )
        .await
        .unwrap()
        {
            CommitOutcome::Committed(_) => {}
            other => panic!("{other:?}"),
        }
    }
    append(&src, events(10..20)).await;
    run_once(&sql, 1).await.unwrap(); // `--once`: process everything available now
    assert_eq!(table_rows(&loc(&out), &["id".to_string()]).await.len(), 20, "--once run stopped early");
}

/// `txn_map` after checkpoints: the sink is created with a checkpoint every
/// 2 commits; restarted workers must still find their stream progress.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_progress_survives_target_checkpoints() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(vec![
        datafusion::arrow::datatypes::Field::new("id", datafusion::arrow::datatypes::DataType::Int64, true),
        datafusion::arrow::datatypes::Field::new("cid", datafusion::arrow::datatypes::DataType::Int64, true),
    ]));
    let kernel: StructType = schema.as_ref().try_into_kernel().unwrap();
    let url = delta::table_url(&loc(&out)).unwrap();
    let table = deltalake_core::DeltaTableBuilder::from_url(url).unwrap().build().unwrap();
    CreateBuilder::new()
        .with_log_store(table.log_store())
        .with_columns(kernel.fields().cloned())
        .with_configuration([("delta.checkpointInterval", Some("2"))])
        .await
        .unwrap();
    let mut all = Vec::new();
    for p in 0..3 {
        let (lines, batch) = clicks(p * 100..p * 100 + 9);
        append_jsonl(&stream, p as i32, &lines);
        all.push(batch);
    }
    let sql = format!(
        "SET nodra.max_records_per_partition = '2'; INSERT INTO delta('{}') SELECT id, cid \
         FROM jsonl_stream('{}', schema => 'id BIGINT, cid BIGINT, user VARCHAR')",
        loc(&out),
        loc(&stream)
    );
    for round in 0..3 {
        run_once(&sql, 2).await.unwrap();
        for p in 0..3 {
            let (lines, batch) = clicks(p * 100 + 9 + round * 3..p * 100 + 12 + round * 3);
            append_jsonl(&stream, p as i32, &lines);
            all.push(batch);
        }
    }
    run_once(&sql, 2).await.unwrap();
    let checkpoints = std::fs::read_dir(out.join("_delta_log"))
        .unwrap()
        .filter(|e| e.as_ref().unwrap().file_name().to_string_lossy().contains(".checkpoint."))
        .count();
    assert!(checkpoints > 3, "{checkpoints}");
    assert_eq!(assert_matches_reference(&sql, &[concat(&all)]).await, 54);
}
