"""Spark side of the Nodra-vs-Spark benchmark.

Runs the same two jobs as `examples/bench.rs` on the same Delta input tables with Spark in
local mode and prints machine-readable timing lines (`RESULT <key> <seconds>`), then checks
that Spark's daily aggregation matches Nodra's (`gold`).

Usage (normally via bench/run.sh):
    uv run --with delta-spark==4.0.1 --with "pyspark>=4.0,<4.1" bench/spark_bench.py <dir> <cores>
"""

import sys
import time

t0 = time.perf_counter()

from delta import configure_spark_with_delta_pip  # noqa: E402
from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402

base = sys.argv[1].rstrip("/")
cores = int(sys.argv[2])
mem = sys.argv[3] if len(sys.argv) > 3 else "8g"

builder = (
    SparkSession.builder.appName("nodra-bench-spark")
    .master(f"local[{cores}]")
    .config("spark.driver.memory", mem)
    .config("spark.sql.shuffle.partitions", str(cores * 2))
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
print(f"RESULT spark_startup {time.perf_counter() - t0:.2f}", flush=True)

events = spark.read.format("delta").load(f"{base}/events")
campaigns = spark.read.format("delta").load(f"{base}/campaigns")
joined = events.alias("e").join(campaigns.alias("c"), F.col("e.campaign_id") == F.col("c.campaign_id"))

# Job 1: enrichment (Nodra class 2)
t = time.perf_counter()
(
    joined.select("e.event_id", "e.ts", "e.user_id", "e.value", "c.brand", "e.day")
    .write.format("delta").mode("overwrite").partitionBy("day")
    .save(f"{base}/silver_spark")
)
print(f"RESULT spark_enrich {time.perf_counter() - t:.2f}", flush=True)

# Job 2: daily aggregation (Nodra class 3)
t = time.perf_counter()
(
    joined.groupBy("e.day", "c.brand")
    .agg(
        F.count(F.lit(1)).alias("events"),
        F.sum("e.value").alias("value"),
        # rsd 1 % (Spark's default is 5 %) to match DataFusion's HLL precision (p = 14, ~0.8 %).
        F.approx_count_distinct("e.user_id", 0.01).alias("users"),
    )
    .write.format("delta").mode("overwrite").partitionBy("day")
    .save(f"{base}/gold_spark")
)
print(f"RESULT spark_daily {time.perf_counter() - t:.2f}", flush=True)

# Correctness: Nodra's gold must equal Spark's on events and value, and both engines' approximate
# distinct users must be within 3 % of the exact count (they use different HLL implementations).
s = spark.read.format("delta").load(f"{base}/gold_spark").alias("s")
n = spark.read.format("delta").load(f"{base}/gold").alias("n")
x = joined.groupBy(F.col("e.day").alias("day"), F.col("c.brand").alias("brand")).agg(
    F.countDistinct("e.user_id").alias("exact_users")
).alias("x")
cmp = s.join(n, ["day", "brand"], "full_outer").join(x, ["day", "brand"], "full_outer")
bad_rows = cmp.where(
    F.col("s.events").isNull() | F.col("n.events").isNull()
    | (F.col("s.events") != F.col("n.events"))
    | (F.abs(F.col("s.value") - F.col("n.value")) > 1e-6 * F.abs(F.col("s.value")) + 1e-9)
    | (F.abs(F.col("n.users") - F.col("x.exact_users")) > 0.03 * F.col("x.exact_users"))
    | (F.abs(F.col("s.users") - F.col("x.exact_users")) > 0.03 * F.col("x.exact_users"))
).select(
    "day", "brand",
    F.col("n.events").alias("nodra_events"), F.col("s.events").alias("spark_events"),
    F.col("n.value").alias("nodra_value"), F.col("s.value").alias("spark_value"),
    F.col("n.users").alias("nodra_users"), F.col("s.users").alias("spark_users"), "x.exact_users",
)
bad = bad_rows.count()
if bad:
    bad_rows.orderBy("day", "brand").show(10, truncate=False)
err = cmp.select(
    F.max(F.abs(F.col("n.users") / F.col("x.exact_users") - 1)).alias("n"),
    F.max(F.abs(F.col("s.users") / F.col("x.exact_users") - 1)).alias("s"),
).first()
print(f"CHECK max distinct-users error vs exact: nodra {err['n']:.2%}, spark {err['s']:.2%}")
groups = s.count()
silver_rows = spark.read.format("delta").load(f"{base}/silver").count()
silver_spark_rows = spark.read.format("delta").load(f"{base}/silver_spark").count()
ok = bad == 0 and groups > 0 and silver_rows == silver_spark_rows
print(f"CHECK groups={groups} mismatching={bad} silver_rows={silver_rows} silver_spark_rows={silver_spark_rows}")
print(f"RESULT check {'ok' if ok else 'FAILED'}", flush=True)
spark.stop()
sys.exit(0 if ok else 1)
