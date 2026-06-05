#!/bin/bash
# bench_metrics.sh — sweep JOB queries × {baseline, robust+join_order} and capture
# six metrics from each profile JSON: total_memory_allocated, cumulative_rows_scanned,
# cumulative_cardinality, system_peak_buffer_memory, sum-of-HASH_JOIN-cardinality
# (deterministic), max-HASH_JOIN-cardinality (deterministic). Output: <out>/metrics.csv
#
# Note: system_peak_buffer_memory is parallelism/timing-sensitive — treat as advisory.
# The HJ cardinality metrics are deterministic given the same plan.
#
# CSV columns:
#   query, baseline_memory, robust_memory,
#          baseline_rows_scanned, robust_rows_scanned,
#          baseline_cardinality, robust_cardinality,
#          baseline_peak_buffer, robust_peak_buffer,
#          baseline_hj_card_sum, robust_hj_card_sum,
#          baseline_hj_card_max, robust_hj_card_max
#
# Usage:
#   ./scripts/bench_metrics.sh                             # all JOB queries
#   ./scripts/bench_metrics.sh --pattern '13.*'            # subset
#   ./scripts/bench_metrics.sh --query 13a                 # single query
#   ./scripts/bench_metrics.sh --out benchmark_results     # output dir

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

DUCKDB="$PROJECT_ROOT/build/release/duckdb"
EXT="$PROJECT_ROOT/build/release/extension/robust/robust.duckdb_extension"
DB="$PROJECT_ROOT/jobdata/imdb.duckdb"
QUERIES_DIR="$PROJECT_ROOT/jobdata/queries"

PATTERN=""
SPECIFIC_QUERY=""
OUT_DIR="$PROJECT_ROOT/benchmark_results"

while [[ $# -gt 0 ]]; do
    case $1 in
        --pattern) PATTERN="$2"; shift 2 ;;
        --query)   SPECIFIC_QUERY="$2"; shift 2 ;;
        --out)     OUT_DIR="$2"; shift 2 ;;
        -h|--help) sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

for f in "$DUCKDB" "$EXT" "$DB"; do
    [ -f "$f" ] || { echo "Missing: $f"; exit 1; }
done

mkdir -p "$OUT_DIR"
OUT_CSV="$OUT_DIR/metrics.csv"
TMP_BASE=$(mktemp /tmp/metrics_base_XXXXXX.json)
TMP_ROBUST=$(mktemp /tmp/metrics_robust_XXXXXX.json)
trap 'rm -f "$TMP_BASE" "$TMP_ROBUST" "$TMP_DB"' EXIT

# if the db is locked, copy it to a temp file
ACTUAL_DB="$DB"
TMP_DB=""
if ! "$DUCKDB" "$DB" -unsigned -readonly -c "SELECT 1;" > /dev/null 2>&1; then
    TMP_DB=$(mktemp /tmp/job_metrics_XXXXXX.duckdb)
    cp "$DB" "$TMP_DB"
    ACTUAL_DB="$TMP_DB"
    echo "(database locked, using temp copy)"
fi

# build query list
if [ -n "$SPECIFIC_QUERY" ]; then
    QUERY_FILES="$QUERIES_DIR/${SPECIFIC_QUERY}.sql"
    [ -f "$QUERY_FILES" ] || { echo "Query not found: $QUERY_FILES"; exit 1; }
elif [ -n "$PATTERN" ]; then
    QUERY_FILES=$(ls -1 "$QUERIES_DIR"/*.sql | grep -E "$PATTERN" || true)
else
    QUERY_FILES=$(ls -1 "$QUERIES_DIR"/*.sql | sort -V)
fi
[ -n "$QUERY_FILES" ] || { echo "No queries matched."; exit 1; }

echo "query,baseline_memory,robust_memory,baseline_rows_scanned,robust_rows_scanned,baseline_cardinality,robust_cardinality,baseline_peak_buffer,robust_peak_buffer,baseline_hj_card_sum,robust_hj_card_sum,baseline_hj_card_max,robust_hj_card_max" > "$OUT_CSV"

# extract six metrics from a profile JSON: 4 top-level + 2 derived from walking the
# operator tree (HASH_JOIN cardinality sum and max).
extract_metrics() {
    python3 -c "
import json, sys
d = json.load(open(sys.argv[1]))

def hj_walk(node, agg):
    name = node.get('operator_name') or node.get('operator_type')
    if name == 'HASH_JOIN':
        c = node.get('operator_cardinality', 0)
        agg[0] += c
        if c > agg[1]:
            agg[1] = c
    for ch in node.get('children', []):
        hj_walk(ch, agg)

root = d.get('children', [d])[0] if 'children' in d else d
agg = [0, 0]
hj_walk(root, agg)
print(','.join(str(x) for x in (
    d.get('total_memory_allocated', 0),
    d.get('cumulative_rows_scanned', 0),
    d.get('cumulative_cardinality', 0),
    d.get('system_peak_buffer_memory', 0),
    agg[0], agg[1])))
" "$1"
}

TOTAL=$(echo "$QUERY_FILES" | wc -w | tr -d ' ')
IDX=0
for qf in $QUERY_FILES; do
    qname=$(basename "$qf" .sql)
    IDX=$((IDX + 1))
    echo -n "[$IDX/$TOTAL] $qname ... "
    QUERY_SQL=$(cat "$qf")

    # baseline: jfp on, no extension
    "$DUCKDB" "$ACTUAL_DB" -unsigned -readonly -c "
PRAGMA enable_profiling='json';
PRAGMA profiling_output='$TMP_BASE';
$QUERY_SQL
" > /dev/null 2>/dev/null || { echo "baseline failed"; continue; }

    # robust: jfp off, extension loaded, join_order heuristic
    "$DUCKDB" "$ACTUAL_DB" -unsigned -readonly -c "
SET disabled_optimizers = 'join_filter_pushdown';
LOAD '$EXT';
SET robust_heuristic = 'join_order';
PRAGMA enable_profiling='json';
PRAGMA profiling_output='$TMP_ROBUST';
$QUERY_SQL
" > /dev/null 2>/dev/null || { echo "robust failed"; continue; }

    # extract_metrics emits 6 fields: mem,rows,card,peak_buf,hj_sum,hj_max
    IFS=',' read -r BMEM BROWS BCARD BPEAK BHJSUM BHJMAX <<< "$(extract_metrics "$TMP_BASE")"
    IFS=',' read -r RMEM RROWS RCARD RPEAK RHJSUM RHJMAX <<< "$(extract_metrics "$TMP_ROBUST")"
    echo "$qname,$BMEM,$RMEM,$BROWS,$RROWS,$BCARD,$RCARD,$BPEAK,$RPEAK,$BHJSUM,$RHJSUM,$BHJMAX,$RHJMAX" >> "$OUT_CSV"
    echo "mem=${BMEM}/${RMEM}  hj_sum=${BHJSUM}/${RHJSUM}  hj_max=${BHJMAX}/${RHJMAX}"
done

echo ""
echo "Done. Wrote $OUT_CSV"
echo "  rows: $(($(wc -l < "$OUT_CSV") - 1))"
