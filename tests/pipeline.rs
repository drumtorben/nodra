mod common;

use common::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class1_three_workers_match_reference() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("events");
    let out = dir.path().join("out");
    events_table(&src, 4, 50).await;
    let sql = format!(
        "INSERT INTO delta('{}', partition_by => 'day') SELECT id, cid, v * 2 AS v2, day FROM delta('{}') WHERE v > 10",
        loc(&out),
        loc(&src)
    );
    run_once(&sql, 3).await.unwrap();
    let n = assert_matches_reference(&sql, &[]).await;
    assert!(n > 100, "{n}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class2_broadcast_join_matches_reference() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dimp, out) = (dir.path().join("events"), dir.path().join("dim"), dir.path().join("out"));
    events_table(&src, 3, 40).await;
    dim_table(&dimp).await;
    for join in ["JOIN", "LEFT JOIN"] {
        let out = out.join(join.replace(' ', "_"));
        let sql = format!(
            "INSERT INTO delta('{}') SELECT e.id, e.day, d.brand FROM delta('{}') e {join} delta('{}') d ON e.cid = d.cid",
            loc(&out),
            loc(&src),
            loc(&dimp)
        );
        let p = run_once(&sql, 2).await.unwrap();
        assert_eq!(p.plan.class, nodra::planner::JobClass::BroadcastJoin);
        assert_matches_reference(&sql, &[]).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn class3_aggregation_matches_reference() {
    let dir = tempfile::tempdir().unwrap();
    let (src, dimp, out) = (dir.path().join("events"), dir.path().join("dim"), dir.path().join("out"));
    events_table(&src, 4, 60).await;
    dim_table(&dimp).await;
    let sql = format!(
        "INSERT INTO delta('{}', partition_by => 'brand') \
         SELECT d.brand, e.day, count(*) AS n, sum(e.v) AS s, avg(e.v) AS a, min(e.id) AS lo, max(e.id) AS hi, \
                count(DISTINCT e.user) AS users, approx_distinct(e.user) AS approx_users \
         FROM delta('{}') e JOIN delta('{}') d ON e.cid = d.cid GROUP BY d.brand, e.day",
        loc(&out),
        loc(&src),
        loc(&dimp)
    );
    let p = run_once(&sql, 3).await.unwrap();
    assert_eq!(p.plan.class, nodra::planner::JobClass::MergeableAggregation);
    let n = assert_matches_reference(&sql, &[]).await;
    assert_eq!(n, 9); // 3 brands x 3 days
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn incremental_runs_add_only_new_data() {
    let dir = tempfile::tempdir().unwrap();
    let (src, out, agg) = (dir.path().join("events"), dir.path().join("out"), dir.path().join("agg"));
    events_table(&src, 2, 30).await;
    let copy = format!("INSERT INTO delta('{}') SELECT id, v FROM delta('{}')", loc(&out), loc(&src));
    let counts = format!(
        "INSERT INTO delta('{}', partition_by => 'day') SELECT day, count(*) AS n, sum(v) AS s FROM delta('{}') GROUP BY day",
        loc(&agg),
        loc(&src)
    );
    run_once(&copy, 2).await.unwrap();
    run_once(&counts, 2).await.unwrap();
    assert_matches_reference(&copy, &[]).await;
    assert_matches_reference(&counts, &[]).await;
    // New data arrives in two more commits; a re-run must add exactly that.
    append(&src, events(60..100)).await;
    append(&src, events(100..130)).await;
    run_once(&copy, 2).await.unwrap();
    run_once(&counts, 2).await.unwrap();
    assert_eq!(assert_matches_reference(&copy, &[]).await, 130);
    assert_eq!(assert_matches_reference(&counts, &[]).await, 3);
    // Nothing new: another run changes nothing.
    run_once(&copy, 2).await.unwrap();
    assert_eq!(assert_matches_reference(&copy, &[]).await, 130);
}
