//! Partial-state round trips through the partials Delta table (types, lists, views).
mod common;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_aggregates_survive_incremental_merges() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out) = (dir.path().join("events"), dir.path().join("out"));
    events_table(&src, 2, 40).await;
    let sql = format!(
        "SET nodra.max_files_per_batch = '2'; \
         INSERT INTO delta('{}', partition_by => 'day') SELECT day, \
           count(*) AS n, avg(CAST(v AS DECIMAL(12,2))) AS avg_dec, stddev(v) AS sd, median(v) AS med, \
           bool_and(v > 3) AS all_big, min(user) AS min_user, max(CAST(id AS SMALLINT)) AS max_small, \
           count(DISTINCT user) AS du, sum(DISTINCT cid) AS dc, \
           first_value(id ORDER BY v DESC) AS top_id, var_pop(v) AS vp, \
           approx_distinct(user) AS ad \
         FROM delta('{}') GROUP BY day",
        loc(&out),
        loc(&src)
    );
    run_once(&sql, 3).await.unwrap();
    assert_eq!(assert_matches_reference(&sql, &[]).await, 3);
    append(&src, events(80..95)).await;
    run_once(&sql, 3).await.unwrap();
    assert_eq!(assert_matches_reference(&sql, &[]).await, 3);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stream_varchar_group_keys_and_distinct_states() {
    let dir = tempfile::tempdir().unwrap();
    let (stream, out) = (dir.path().join("clicks"), dir.path().join("out"));
    let mut all = Vec::new();
    for p in 0..3 {
        let (lines, batch) = clicks(p * 100..p * 100 + 20);
        append_jsonl(&stream, p as i32, &lines);
        all.push(batch);
    }
    let sql = format!(
        "SET nodra.max_records_per_partition = '6'; \
         INSERT INTO delta('{}', partition_by => 'user') SELECT user, count(*) AS n, count(DISTINCT cid) AS dc, \
           max(id) AS hi FROM jsonl_stream('{}', schema => 'id BIGINT, cid BIGINT, user VARCHAR') GROUP BY user",
        loc(&out),
        loc(&stream)
    );
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[concat(&all)]).await;
    for p in [0, 2] {
        let (lines, batch) = clicks(p * 100 + 20..p * 100 + 31);
        append_jsonl(&stream, p as i32, &lines);
        all.push(batch);
    }
    run_once(&sql, 2).await.unwrap();
    assert_matches_reference(&sql, &[concat(&all)]).await;
}
