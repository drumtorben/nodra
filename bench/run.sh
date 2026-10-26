#!/usr/bin/env bash
# Nodra vs. Spark (local mode) on the same machine, the same Delta input and the same two jobs.
#
#   bench/run.sh                         # defaults below
#   ROWS=100000000 RUNS=5 bench/run.sh   # bigger
#
# Needs: cargo, uv, Java 17+ (Spark 4). Writes bench/results/<date>-<host>.md.
# Plain bash 3.2 so it runs with macOS's /bin/bash.
set -euo pipefail

ROWS=${ROWS:-40000000}          # events
FILES=${FILES:-40}              # Delta commits / files in the input table
RUNS=${RUNS:-3}                 # measured runs per engine (median is reported), after one warm-up
DIR=${DIR:-/tmp/nodra-bench-$ROWS}
SPARK_MEM=${SPARK_MEM:-16g}
DELTA_SPARK=${DELTA_SPARK:-4.0.1}
# delta-spark does not cap its pyspark dependency, so pin the Spark line its jars were built for.
PYSPARK=${PYSPARK:-">=4.0,<4.1"}
if [ -z "${CORES:-}" ]; then
  CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc)
fi

cd "$(dirname "$0")/.."
ROOT=$(pwd)

need() { command -v "$1" >/dev/null 2>&1 || { echo "missing: $1 — $2" >&2; exit 1; }; }
need cargo "install Rust via https://rustup.rs"
need uv "brew install uv"
need java "brew install openjdk@21 and put it on PATH / set JAVA_HOME"
JAVA_MAJOR=$(java -version 2>&1 | awk -F'"' '/version/ {split($2, v, "."); print v[1]; exit}')
if [ "${JAVA_MAJOR:-0}" -lt 17 ]; then echo "Spark 4 needs Java 17+, found $JAVA_MAJOR" >&2; exit 1; fi

# Peak RSS of a command including all its child processes (Spark = Python + JVM),
# sampled every 200 ms. Prints the command's output; leaves the peak in $PEAK_MB.
tree_rss_kb() {
  ps -A -o pid=,ppid=,rss= | awk -v root="$1" '
    { pp[$1] = $2; rss[$1] = $3 }
    END {
      total = 0
      for (p in rss) { q = p; while (q != "" && q != 0 && q != 1) { if (q == root) { total += rss[p]; break } q = pp[q] } }
      print total
    }'
}
measure() {
  local out=$1; shift
  "$@" >"$out" 2>&1 &
  local pid=$! peak=0 now
  while kill -0 "$pid" 2>/dev/null; do
    now=$(tree_rss_kb "$pid"); now=${now:-0}; [ "$now" -gt "$peak" ] && peak=$now
    sleep 0.2
  done
  local rc=0; wait "$pid" || rc=$?
  PEAK_MB=$((peak / 1024))
  if [ $rc -ne 0 ]; then
    echo "command failed ($rc); first errors:" >&2
    grep -m 5 -E "Error|Exception|Caused by" "$out" >&2 || true
    grep -E "^(CHECK|[|+])" "$out" >&2 || true
    echo "--- last lines:" >&2; tail -15 "$out" >&2; echo "full log: $out" >&2; exit $rc
  fi
}
val() { awk -v k="$1" '$1 == "RESULT" && $2 == k { print $3 }' "$2"; }
median() { sort -n | awk '{ a[NR] = $1 } END { print (NR % 2) ? a[(NR + 1) / 2] : (a[NR / 2] + a[NR / 2 + 1]) / 2 }'; }

echo "== building Nodra (release)"
cargo build --release --example bench
BENCH="$ROOT/target/release/examples/bench"
SPARK=(uv run --python 3.12 --with "delta-spark==$DELTA_SPARK" --with "pyspark$PYSPARK" "$ROOT/bench/spark_bench.py" "$DIR" "$CORES" "$SPARK_MEM")

TMP=$(mktemp -d)
echo "== warm-up (generates $ROWS rows into $DIR on the first run, fetches Spark/Delta jars)"
measure "$TMP/warm_nodra" "$BENCH" "$DIR" "$ROWS" "$FILES" "$CORES"
grep -h "generated" "$TMP/warm_nodra" || true
measure "$TMP/warm_spark" "${SPARK[@]}"
grep -h "CHECK" "$TMP/warm_spark"

for i in $(seq 1 "$RUNS"); do
  echo "== run $i/$RUNS"
  measure "$TMP/nodra_$i" "$BENCH" "$DIR" "$ROWS" "$FILES" "$CORES"; echo "$PEAK_MB" >"$TMP/nodra_mem_$i"
  measure "$TMP/spark_$i" "${SPARK[@]}";                             echo "$PEAK_MB" >"$TMP/spark_mem_$i"
  echo "   nodra: enrich $(val nodra_enrich "$TMP/nodra_$i")s, daily $(val nodra_daily "$TMP/nodra_$i")s, peak $(cat "$TMP/nodra_mem_$i") MB"
  echo "   spark: startup $(val spark_startup "$TMP/spark_$i")s, enrich $(val spark_enrich "$TMP/spark_$i")s, daily $(val spark_daily "$TMP/spark_$i")s, peak $(cat "$TMP/spark_mem_$i") MB, check $(val check "$TMP/spark_$i")"
done

med() { for i in $(seq 1 "$RUNS"); do val "$1" "$TMP/$2_$i"; done | median; }
memmed() { cat "$TMP"/$1_mem_* | median; }
NE=$(med nodra_enrich nodra); ND=$(med nodra_daily nodra)
SS=$(med spark_startup spark); SE=$(med spark_enrich spark); SD=$(med spark_daily spark)
NM=$(memmed nodra); SM=$(memmed spark)
# ratio <spark> <nodra> <word if Nodra is better> <word if worse>, e.g. "3.8× faster" / "1.3× slower"
ratio() {
  awk -v a="$1" -v b="$2" -v up="${3:-faster}" -v down="${4:-slower}" \
    'BEGIN { if (a >= b) printf "%.1f× %s", a / b, up; else printf "%.1f× %s", b / a, down }'
}
row_rate() { awk -v r="$ROWS" -v s="$1" 'BEGIN { printf "%.1f", r / s / 1e6 }'; }

HOST=$(scutil --get ComputerName 2>/dev/null || hostname -s)
CPU=$(sysctl -n machdep.cpu.brand_string 2>/dev/null || lscpu | awk -F: '/Model name/ { gsub(/^ +/, "", $2); print $2; exit }')
MEM_GB=$(( $( (sysctl -n hw.memsize 2>/dev/null || awk '/MemTotal/ { print $2 * 1024 }' /proc/meminfo) ) / 1073741824 ))
INPUT=$(du -sh "$DIR/events" | cut -f1)
mkdir -p bench/results
# A non-default Spark heap gets its own file instead of overwriting the default run's.
SUFFIX=""
[ "$SPARK_MEM" != 16g ] && SUFFIX="-spark$SPARK_MEM"
OUT="bench/results/$(date +%Y-%m-%d)-$(echo "$HOST" | tr ' ' '-' | tr -cd '[:alnum:]-_')-$((ROWS / 1000000))M$SUFFIX.md"
cat >"$OUT" <<MD
# Nodra vs. Spark — $(date +%Y-%m-%d)

| | |
|---|---|
| Machine | $CPU, $MEM_GB GB RAM ($HOST) |
| Input | $ROWS events in $FILES Delta commits, 4 day partitions ($INPUT on disk), 1 000-row campaign table |
| Parallelism | Nodra $CORES local workers · Spark local[$CORES], $SPARK_MEM driver memory |
| Versions | Nodra $(git rev-parse --short HEAD) · Spark $(uv run --python 3.12 --with "pyspark$PYSPARK" python -c "import pyspark; print(pyspark.__version__)" 2>/dev/null) · delta-spark $DELTA_SPARK · $(java -version 2>&1 | head -1) |
| Runs | median of $RUNS after one warm-up; Spark starts a fresh JVM each run like a scheduled job |
| Correctness | Spark's results compared with Nodra's: **$(val check "$TMP/spark_$RUNS")** |

| Job | Nodra | Spark | Nodra vs. Spark |
|---|---:|---:|---:|
| Enrichment (broadcast join, write partitioned Delta) | ${NE}s ($(row_rate "$NE") M rows/s) | ${SE}s ($(row_rate "$SE") M rows/s) | $(ratio "$SE" "$NE") |
| Daily aggregation (join + group by + approx distinct) | ${ND}s ($(row_rate "$ND") M rows/s) | ${SD}s ($(row_rate "$SD") M rows/s) | $(ratio "$SD" "$ND") |
| Both jobs, incl. Spark session startup (${SS}s) | $(awk -v a="$NE" -v b="$ND" 'BEGIN { print a + b }')s | $(awk -v a="$SS" -v b="$SE" -v c="$SD" 'BEGIN { print a + b + c }')s | $(ratio "$(awk -v a="$SS" -v b="$SE" -v c="$SD" 'BEGIN { print a + b + c }')" "$(awk -v a="$NE" -v b="$ND" 'BEGIN { print a + b }')") |
| Peak memory (whole process tree) | $NM MB | $SM MB | $(ratio "$SM" "$NM" less more) |

Reproduce: \`ROWS=$ROWS FILES=$FILES CORES=$CORES RUNS=$RUNS SPARK_MEM=$SPARK_MEM bench/run.sh\`
MD
echo
cat "$OUT"
echo
echo "written to $OUT"
rm -rf "$TMP"
