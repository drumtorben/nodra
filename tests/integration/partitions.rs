//! Class-4 partition progress and class-3 partition-scoped merges.
use crate::common;

use std::sync::Arc;

use common::*;
use datafusion::arrow::array::{ArrayRef, Float64Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use nodra::planner::{JobClass, MergeScope};

fn spend(day: &str, n: i64) -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("cid", DataType::Int64, true),
            Field::new("w", DataType::Float64, true),
            Field::new("day", DataType::Utf8, true),
        ])),
        vec![
            Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(Float64Array::from((0..n).map(|c| c as f64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(vec![day; n as usize])),
        ],
    )
    .unwrap()
}

fn with_day(b: RecordBatch, day: Option<&str>) -> RecordBatch {
    let mut cols = b.columns().to_vec();
    cols[4] = Arc::new(StringArray::from(vec![day; b.num_rows()])) as ArrayRef;
    RecordBatch::try_new(b.schema(), cols).unwrap()
}

/// Class 4 leaves a partition pending while it exists in only one of the two tables, so the
/// other side's files arriving later are still joined.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class4_partition_present_on_one_side_stays_pending() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, out) = (dir.path().join("a"), dir.path().join("b"), dir.path().join("out"));
    events_table(&a, 3, 30).await; // days 2026-10-10..12
    create_table(&b, &spend("x", 1).schema(), &["day"]).await;
    for d in ["2026-10-10", "2026-10-11", "2026-10-12"] {
        append(&b, spend(d, 7)).await;
    }
    let sql = format!(
        "SET nodra.broadcast_threshold = '1KB'; INSERT INTO delta('{}', partition_by => 'day') \
         SELECT a.day, a.id, b.w FROM delta('{}') a JOIN delta('{}') b ON a.day = b.day AND a.cid = b.cid",
        loc(&out),
        loc(&a),
        loc(&b)
    );
    let p = run_once(&sql, 2).await.unwrap();
    assert_eq!(p.plan.class, JobClass::CoPartitionedJoin);
    // Day 13 lands in `a` first ...
    append(&a, with_day(events(1000..1030), Some("2026-10-13"))).await;
    run_once(&sql, 2).await.unwrap();
    // ... and in `b` an hour later.
    append(&b, spend("2026-10-13", 7)).await;
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
}

/// Partition-scoped merge with a NULL partition and incremental updates of single partitions.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scoped_merge_with_null_and_date_partitions() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out, out2) = (dir.path().join("events"), dir.path().join("out"), dir.path().join("out2"));
    events_table(&src, 2, 30).await;
    append(&src, with_day(events(200..210), None)).await;
    let by_day = format!(
        "INSERT INTO delta('{}', partition_by => 'day') SELECT day, count(*) AS n, sum(v) AS s FROM delta('{}') GROUP BY day",
        loc(&out),
        loc(&src)
    );
    let by_date = format!(
        "INSERT INTO delta('{}', partition_by => 'd') SELECT CAST(day AS DATE) AS d, count(DISTINCT user) AS u \
         FROM delta('{}') GROUP BY CAST(day AS DATE)",
        loc(&out2),
        loc(&src)
    );
    for sql in [&by_day, &by_date] {
        let p = run_once(sql, 2).await.unwrap();
        assert!(matches!(p.plan.merge_scope, Some(MergeScope::Partitions(_))), "{:?}", p.plan.merge_scope);
    }
    for (batch, label) in [(with_day(events(300..310), None), "null"), (with_day(events(400..405), Some("2026-10-11")), "day")] {
        append(&src, batch).await;
        for sql in [&by_day, &by_date] {
            run_once(sql, 2).await.unwrap();
            let n = assert_matches_reference(sql, &[]).await;
            assert_eq!(n, 4, "{label}");
        }
    }
}
