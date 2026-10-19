//! Failure handling: crashes, zombie workers, source changes (rescaling: `rescale*`).
use crate::common;

use std::sync::Arc;

use common::*;
use nodra::exec::{self, Pipeline, WorkerId};
use nodra::job::Job;
use tokio::sync::watch;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_before_commit_does_not_duplicate() {
    for class3 in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let (src, out) = (dir.path().join("events"), dir.path().join("out"));
        events_table(&src, 3, 30).await;
        let name = format!("crash{}", class3 as u8);
        let sql = if class3 {
            format!(
                "SET nodra.job_name = '{name}'; INSERT INTO delta('{}') SELECT cid, count(*) AS n FROM delta('{}') GROUP BY cid",
                loc(&out),
                loc(&src)
            )
        } else {
            format!(
                "SET nodra.job_name = '{name}'; INSERT INTO delta('{}') SELECT id, v FROM delta('{}')",
                loc(&out),
                loc(&src)
            )
        };
        exec::testing::fail_before_commit(&name, 1);
        let err = run_once(&sql, 1).await.unwrap_err();
        assert!(err.to_string().contains("injected failure"), "{err}");
        // Files of the failed batch may exist on disk, but nothing was committed.
        run_once(&sql, 1).await.unwrap();
        assert_matches_reference(&sql, &[]).await;
        append(&src, events(90..120)).await;
        exec::testing::fail_before_commit(&name, 1);
        assert!(run_once(&sql, 1).await.is_err());
        run_once(&sql, 1).await.unwrap();
        assert_matches_reference(&sql, &[]).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn zombie_replicas_of_a_worker_never_duplicate() {
    // Two processes believe they are worker 0: both may process the same input, but only one
    // commit of each progress value can succeed.
    for round in 0..4 {
        let dir = tempfile::tempdir().unwrap();
        let (src, out) = (dir.path().join("events"), dir.path().join("out"));
        events_table(&src, 6, 25).await;
        let sql = format!(
            "SET nodra.max_files_per_batch = '2'; INSERT INTO delta('{}') SELECT id, v FROM delta('{}')",
            loc(&out),
            loc(&src)
        );
        let p =
            Arc::new(Pipeline::prepare(Job::parse(&sql, &format!("zombie{round}")).unwrap()).await.unwrap());
        let (_tx, rx) = watch::channel(false);
        let id = WorkerId { index: 0, count: 1 };
        let a = tokio::spawn(exec::run_worker(p.clone(), id, true, rx.clone()));
        let b = tokio::spawn(exec::run_worker(p.clone(), id, true, rx.clone()));
        a.await.unwrap().unwrap();
        b.await.unwrap().unwrap();
        assert_eq!(assert_matches_reference(&sql, &[]).await, 150);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_in_source_is_not_reprocessed() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 3, 20).await;
    let sql = format!("INSERT INTO delta('{}') SELECT id, v FROM delta('{}')", loc(&out), loc(&src));
    run_once(&sql, 2).await.unwrap();
    assert!(compact(&src).await);
    append(&src, events(60..70)).await;
    run_once(&sql, 2).await.unwrap();
    assert_eq!(assert_matches_reference(&sql, &[]).await, 70);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deletes_in_source_fail_unless_ignored() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 3, 20).await;
    let sql = format!("INSERT INTO delta('{}') SELECT id FROM delta('{}')", loc(&out), loc(&src));
    run_once(&sql, 1).await.unwrap();
    delete_one_file(&src).await;
    append(&src, events(60..65)).await;
    let err = run_once(&sql, 1).await.unwrap_err().to_string();
    assert!(err.contains("deletes or updates"), "{err}");
    let ignoring = format!("SET nodra.ignore_changes = true; {sql}");
    run_once(&ignoring, 1).await.unwrap();
    // 60 original rows + 5 new ones; the deleted rows stay in the sink (changes are ignored).
    assert_eq!(table_rows(&loc(&out), &["id".to_string()]).await.len(), 65);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn source_schema_change_stops_the_job() {
    use deltalake_core::writer::{DeltaWriter, RecordBatchWriter, WriteMode};
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    // Unpartitioned: delta-rs cannot merge schemas of partitioned tables.
    create_table(&src, &events_schema(), &[]).await;
    append(&src, events(0..10)).await;
    let sql = format!("INSERT INTO delta('{}') SELECT id FROM delta('{}')", loc(&out), loc(&src));
    let job = Job::parse(&sql, "schema").unwrap();
    let p = Arc::new(Pipeline::prepare(job).await.unwrap());
    // Evolve the source schema after planning.
    let mut table = nodra::delta::open(&loc(&src)).await.unwrap();
    let b = events(10..12);
    let mut fields: Vec<_> = b.schema().fields().iter().cloned().collect();
    fields.push(Arc::new(datafusion::arrow::datatypes::Field::new(
        "extra",
        datafusion::arrow::datatypes::DataType::Utf8,
        true,
    )));
    let mut cols = b.columns().to_vec();
    cols.push(Arc::new(datafusion::arrow::array::StringArray::from(vec![Some("x"), None])));
    let b2 = datafusion::arrow::array::RecordBatch::try_new(
        Arc::new(datafusion::arrow::datatypes::Schema::new(fields)),
        cols,
    )
    .unwrap();
    let mut w = RecordBatchWriter::for_table(&table).unwrap();
    w.write_with_mode(b2, WriteMode::MergeSchema).await.unwrap();
    w.flush_and_commit(&mut table).await.unwrap();
    let (_tx, rx) = watch::channel(false);
    let err = exec::run(p, vec![0], 1, true, rx).await.unwrap_err().to_string();
    assert!(err.contains("schema"), "{err}");
}
