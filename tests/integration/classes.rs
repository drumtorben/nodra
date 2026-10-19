//! Class 4, partition-scoped merges, partition value encoding, overwrite mode.
use crate::common;

use std::sync::Arc;

use common::*;
use datafusion::arrow::array::{ArrayRef, Date32Array, Decimal128Array, Float64Array, Int64Array, RecordBatch, StringArray, TimestampMicrosecondArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use nodra::planner::{JobClass, MergeScope};

fn spend(days: &[&str], per_day: i64) -> RecordBatch {
    let mut day = Vec::new();
    let mut cid = Vec::new();
    let mut w = Vec::new();
    for d in days {
        for c in 0..per_day {
            day.push(d.to_string());
            cid.push(c);
            w.push(c as f64 * 1.5);
        }
    }
    RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("cid", DataType::Int64, true),
            Field::new("w", DataType::Float64, true),
            Field::new("day", DataType::Utf8, true),
        ])),
        vec![Arc::new(Int64Array::from(cid)) as ArrayRef, Arc::new(Float64Array::from(w)), Arc::new(StringArray::from(day))],
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class4_copartitioned_join() {
    let dir = tempfile::tempdir().unwrap();
    let (a, b, out) = (dir.path().join("a"), dir.path().join("b"), dir.path().join("out"));
    events_table(&a, 3, 30).await; // days 2026-10-10..12
    let sb = spend(&["2026-10-10"], 3).schema();
    create_table(&b, &sb, &["day"]).await;
    append(&b, spend(&["2026-10-10", "2026-10-11", "2026-10-12"], 7)).await;
    let sql = format!(
        "SET nodra.broadcast_threshold = '1KB'; \
         INSERT INTO delta('{}', partition_by => 'day') \
         SELECT a.day, a.cid, sum(a.v) AS v, max(b.w) AS w FROM delta('{}') a JOIN delta('{}') b \
         ON a.day = b.day AND a.cid = b.cid GROUP BY a.day, a.cid",
        loc(&out),
        loc(&a),
        loc(&b)
    );
    let p = run_once(&sql, 2).await.unwrap();
    assert_eq!(p.plan.class, JobClass::CoPartitionedJoin);
    assert_matches_reference(&sql, &[]).await;
    // A new partition arrives in both tables; the already processed ones are not redone.
    let mut new_a = events(1000..1030);
    let day = Arc::new(StringArray::from(vec!["2026-10-13"; 30])) as ArrayRef;
    let mut cols = new_a.columns().to_vec();
    cols[4] = day;
    new_a = RecordBatch::try_new(new_a.schema(), cols).unwrap();
    append(&a, new_a).await;
    append(&b, spend(&["2026-10-13"], 7)).await;
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn partition_scoped_merge_only_rewrites_changed_days() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 3, 30).await;
    let sql = format!(
        "INSERT INTO delta('{}', partition_by => 'day') SELECT day, cid, count(*) AS n FROM delta('{}') GROUP BY day, cid",
        loc(&out),
        loc(&src)
    );
    let p = run_once(&sql, 2).await.unwrap();
    assert!(matches!(p.plan.merge_scope, Some(MergeScope::Partitions(_))));
    assert_matches_reference(&sql, &[]).await;
    let files_before = nodra::delta::active_files(&nodra::delta::open(&loc(&out)).await.unwrap()).unwrap();
    // New rows only for 2026-10-11 (ids with i % 3 == 1).
    let ids: Vec<i64> = (500..560).filter(|i| i % 3 == 1).collect();
    let b = events(0..1);
    let batch = RecordBatch::try_new(
        b.schema(),
        vec![
            Arc::new(Int64Array::from(ids.clone())) as ArrayRef,
            Arc::new(Int64Array::from(ids.iter().map(|i| i % 7).collect::<Vec<_>>())),
            Arc::new(StringArray::from(ids.iter().map(|i| format!("u{i}")).collect::<Vec<_>>())),
            Arc::new(Float64Array::from(ids.iter().map(|i| *i as f64).collect::<Vec<_>>())),
            Arc::new(StringArray::from(vec!["2026-10-11"; ids.len()])),
        ],
    )
    .unwrap();
    append(&src, batch).await;
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[]).await;
    let files_after = nodra::delta::active_files(&nodra::delta::open(&loc(&out)).await.unwrap()).unwrap();
    for f in &files_before {
        let day = f.partition_values[0].as_deref();
        let kept = files_after.iter().any(|g| g.path == f.path);
        assert_eq!(kept, day != Some("2026-10-11"), "{:?} {day:?}", f.path);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn odd_partition_values_and_types_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out, agg) = (dir.path().join("src"), dir.path().join("out"), dir.path().join("agg"));
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, true),
        Field::new("ts", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())), true),
        Field::new("d", DataType::Date32, true),
        Field::new("amount", DataType::Decimal128(10, 2), true),
        Field::new("tag", DataType::Utf8, true),
    ]));
    create_table(&src, &schema, &["tag"]).await;
    let tags = [Some("a/b c"), Some("100%=x"), None, Some("ümlaut"), Some("")];
    let n = tags.len() as i64 * 4;
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from((0..n).collect::<Vec<_>>())) as ArrayRef,
            Arc::new(TimestampMicrosecondArray::from((0..n).map(|i| 1_760_000_000_000_000 + i * 3_600_000_000).collect::<Vec<_>>()).with_timezone("UTC")),
            Arc::new(Date32Array::from((0..n).map(|i| 20_000 + i as i32).collect::<Vec<_>>())),
            Arc::new(Decimal128Array::from((0..n).map(|i| i as i128 * 125).collect::<Vec<_>>()).with_precision_and_scale(10, 2).unwrap()),
            Arc::new(StringArray::from((0..n).map(|i| tags[(i % tags.len() as i64) as usize]).collect::<Vec<_>>())),
        ],
    )
    .unwrap();
    append(&src, batch).await;
    let copy = format!(
        "INSERT INTO delta('{}', partition_by => 'tag') SELECT id, ts, d, amount, tag FROM delta('{}')",
        loc(&out),
        loc(&src)
    );
    run_once(&copy, 2).await.unwrap();
    assert_eq!(assert_matches_reference(&copy, &[]).await, n as usize);
    let sums = format!(
        "INSERT INTO delta('{}', partition_by => 'tag') SELECT tag, sum(amount) AS total, min(ts) AS first, max(d) AS last_day \
         FROM delta('{}') GROUP BY tag",
        loc(&agg),
        loc(&src)
    );
    run_once(&sums, 2).await.unwrap();
    assert_eq!(assert_matches_reference(&sums, &[]).await, tags.len());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn overwrite_mode_replaces_the_table() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 10).await;
    let sql = format!(
        "SET nodra.job_name = 'ow'; INSERT INTO delta('{}', mode => 'overwrite') SELECT id FROM delta('{}')",
        loc(&out),
        loc(&src)
    );
    run_once(&sql, 1).await.unwrap();
    assert_eq!(assert_matches_reference(&sql, &[]).await, 20);
    // Each run recomputes the table from the full source.
    append(&src, events(20..25)).await;
    run_once(&sql, 1).await.unwrap();
    assert_eq!(assert_matches_reference(&sql, &[]).await, 25);
    // Overwrite needs one worker.
    assert!(run_once(&sql, 2).await.is_err());
}
