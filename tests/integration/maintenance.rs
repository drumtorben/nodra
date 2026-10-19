//! Table maintenance running concurrently with Nodra. Merge commits carry `remove` actions
//! computed from their read snapshot, and delta-rs (1.1.2) does not treat a concurrent OPTIMIZE
//! (dataChange=false removes/adds) as a conflict: the merge must recompute its removes on the
//! compacted snapshot, or the compacted copy of the old rows survives next to the new ones.
use crate::common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use nodra::exec::{self, Pipeline, WorkerId};
use nodra::job::Job;
use tokio::sync::watch;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merge_racing_sink_optimize_keeps_one_row_per_group() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 30).await;
    let sql = format!(
        "SET nodra.job_name = 'opt'; INSERT INTO delta('{}') SELECT cid, count(*) AS n FROM delta('{}') GROUP BY cid",
        loc(&out),
        loc(&src)
    );
    run_once(&sql, 1).await.unwrap(); // sink exists, 7 groups
    assert!(compact(&out).await); // sanity: compaction alone keeps the rows
    assert_eq!(table_rows(&loc(&out), &["cid".to_string()]).await.len(), 7);
    let p = Arc::new(Pipeline::prepare(Job::parse(&sql, "x").unwrap()).await.unwrap());
    for round in 0..12u64 {
        append(&src, events(100 + round as i64 * 10..110 + round as i64 * 10)).await;
        let (_tx, rx) = watch::channel(false);
        exec::run_worker(p.clone(), WorkerId { index: 0, count: 1 }, true, rx).await.unwrap(); // partials only
                                                                                               // Someone compacts the sink while the merge step runs.
        let (m, _) = tokio::join!(nodra::merge::merge(&p), async {
            tokio::time::sleep(Duration::from_millis(round * 4)).await;
            compact(&out).await;
        });
        m.unwrap();
        let rows = table_rows(&loc(&out), &["cid".to_string()]).await;
        assert_eq!(rows.len(), 7, "round {round}: sink has {} rows for 7 groups: {rows:?}", rows.len());
    }
}
