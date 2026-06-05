#!/bin/bash
# bench_rg_instrument.sh — sweep JOB queries × {baseline, Robust-largest-root, Robust-join-order}
# with DUCKDB_RG_INSTRUMENT=1 and emit a per-(query,config,table) CSV of row-group counters.
#
# Phase 1 of the row-group instrumentation lives in the duckdb submodule; each scan
# thread emits `[rg_instr] table=<n> visited=N pruned_zonemap=N zero_emit=N ...`
# on destruction. This script aggregates those lines per query/config and joins
# them with total row-group counts pulled from pragma_storage_info().
#
# Output: rg_instrument_results/<timestamp>/results.csv
#   columns: query, config, table, total_rgs, visited, pruned_zonemap,
#            zero_emit, zero_emit_pct
#
# Usage:
#   ./bench_rg_instrument.sh                 # run all JOB queries × all 3 configs
#   ./bench_rg_instrument.sh --limit 5       # first 5 queries
#   ./bench_rg_instrument.sh --query 13a     # single query
#   ./bench_rg_instrument.sh --configs baseline,robust-largest-root

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
DUCKDB="$PROJECT_ROOT/build/release/duckdb"
EXT="$PROJECT_ROOT/build/release/extension/robust/robust.duckdb_extension"
DB="$PROJECT_ROOT/jobdata/imdb.duckdb"
QUERIES_DIR="$PROJECT_ROOT/jobdata/queries"
AGG="$SCRIPT_DIR/rg_aggregate.py"

LIMIT=0
SPECIFIC_QUERY=""
CONFIGS="baseline,robust-largest-root,robust-join-order"
OUT_DIR=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --limit)   LIMIT="$2"; shift 2 ;;
        --query)   SPECIFIC_QUERY="$2"; shift 2 ;;
        --configs) CONFIGS="$2"; shift 2 ;;
        --out)     OUT_DIR="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
            exit 0 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

for f in "$DUCKDB" "$EXT" "$DB" "$AGG"; do
    [ -e "$f" ] || { echo "Missing: $f"; exit 1; }
done

if [ -z "$OUT_DIR" ]; then
    OUT_DIR="$PROJECT_ROOT/rg_instrument_results/$(date +%Y%m%d_%H%M%S)"
fi
mkdir -p "$OUT_DIR"
PER_QUERY_CSV="$OUT_DIR/per_query.csv"
TOTAL_CSV="$OUT_DIR/total_rgs.csv"
RESULT_CSV="$OUT_DIR/results.csv"
RAW_DIR="$OUT_DIR/raw"
mkdir -p "$RAW_DIR"

# if the db is locked, copy it to a temp file
ACTUAL_DB="$DB"
TMP_DB=""
if ! "$DUCKDB" "$DB" -unsigned -readonly -c "SELECT 1;" > /dev/null 2>&1; then
    TMP_DB=$(mktemp /tmp/job_rginstr_XXXXXX.duckdb)
    cp "$DB" "$TMP_DB"
    ACTUAL_DB="$TMP_DB"
    echo "(database locked, using temp copy)"
fi
trap '[ -n "$TMP_DB" ] && rm -f "$TMP_DB"' EXIT

# step 1: dump total row groups per table (loop in shell because
# pragma_storage_info doesn't accept lateral column parameters)
echo "Capturing total row groups per table..."
echo "table,total_rgs" > "$TOTAL_CSV"
TABLES=$("$DUCKDB" "$ACTUAL_DB" -unsigned -readonly -noheader -list -c \
    "SELECT table_name FROM duckdb_tables() WHERE schema_name='main' ORDER BY table_name;")
for tbl in $TABLES; do
    rgs=$("$DUCKDB" "$ACTUAL_DB" -unsigned -readonly -noheader -list -c \
        "SELECT COUNT(DISTINCT row_group_id) FROM pragma_storage_info('$tbl');")
    echo "$tbl,$rgs" >> "$TOTAL_CSV"
done
echo "  wrote $(($(wc -l < "$TOTAL_CSV") - 1)) tables to $TOTAL_CSV"

# step 2: build the query list
if [ -n "$SPECIFIC_QUERY" ]; then
    QUERY_FILES="$QUERIES_DIR/${SPECIFIC_QUERY}.sql"
    [ -f "$QUERY_FILES" ] || { echo "Query not found: $QUERY_FILES"; exit 1; }
else
    QUERY_FILES=$(ls -1 "$QUERIES_DIR"/*.sql | sort -V)
fi

# step 3: per-config setup
config_settings() {
    case "$1" in
        baseline)
            # jfp on, no robust
            echo "" ;;
        robust-largest-root)
            echo "SET disabled_optimizers = 'join_filter_pushdown'; LOAD '$EXT'; SET robust_heuristic = 'largest_root';" ;;
        robust-join-order)
            echo "SET disabled_optimizers = 'join_filter_pushdown'; LOAD '$EXT'; SET robust_heuristic = 'join_order';" ;;
        *) echo "Unknown config: $1" >&2; return 1 ;;
    esac
}

# init per-query CSV with header
> "$PER_QUERY_CSV"
echo "query,config,table,visited,pruned_zonemap,zero_emit,zero_emit_pct" > "$PER_QUERY_CSV"

# step 4: sweep
TOTAL_QUERIES=$(echo "$QUERY_FILES" | wc -w | tr -d ' ')
if [ "$LIMIT" -gt 0 ] && [ "$LIMIT" -lt "$TOTAL_QUERIES" ]; then
    TOTAL_QUERIES=$LIMIT
fi
IFS=',' read -ra CFG_ARR <<< "$CONFIGS"
TOTAL_RUNS=$((TOTAL_QUERIES * ${#CFG_ARR[@]}))
RUN_IDX=0
COUNT=0

for query_file in $QUERY_FILES; do
    query_name=$(basename "$query_file" .sql)
    ((COUNT++))
    if [ "$LIMIT" -gt 0 ] && [ "$COUNT" -gt "$LIMIT" ]; then
        break
    fi
    QUERY_SQL=$(cat "$query_file")

    for cfg in "${CFG_ARR[@]}"; do
        ((RUN_IDX++))
        echo -n "[$RUN_IDX/$TOTAL_RUNS] $query_name @ $cfg ... "

        SETTINGS=$(config_settings "$cfg")
        STDERR_FILE="$RAW_DIR/${query_name}_${cfg}.stderr"

        DUCKDB_RG_INSTRUMENT=1 "$DUCKDB" "$ACTUAL_DB" -unsigned -readonly \
            -c "$SETTINGS $QUERY_SQL" \
            > /dev/null 2> "$STDERR_FILE" || true

        python3 "$AGG" \
            --input "$STDERR_FILE" \
            --query "$query_name" \
            --config "$cfg" \
            --out "$PER_QUERY_CSV"

        rg_lines=$(grep -c '^\[rg_instr\]' "$STDERR_FILE" 2>/dev/null || echo 0)
        echo "($rg_lines rg_instr lines)"
    done
done

# step 5: join per-query totals with total_rgs and emit results.csv
echo ""
echo "Joining with total_rgs..."
"$DUCKDB" -c "
COPY (
    SELECT
        pq.query,
        pq.config,
        pq.\"table\",
        COALESCE(t.total_rgs, 0) AS total_rgs,
        pq.visited,
        pq.pruned_zonemap,
        pq.zero_emit,
        pq.zero_emit_pct
    FROM read_csv('$PER_QUERY_CSV', header=true, delim=',', auto_detect=false,
                  columns={'query':'VARCHAR','config':'VARCHAR','table':'VARCHAR',
                           'visited':'BIGINT','pruned_zonemap':'BIGINT',
                           'zero_emit':'BIGINT','zero_emit_pct':'DOUBLE'}) pq
    LEFT JOIN read_csv('$TOTAL_CSV', header=true, delim=',', auto_detect=false,
                       columns={'table':'VARCHAR','total_rgs':'BIGINT'}) t
      USING (\"table\")
    ORDER BY pq.query, pq.config, pq.\"table\"
) TO '$RESULT_CSV' (HEADER, DELIMITER ',');
" > /dev/null

echo ""
echo "Done."
echo "  per-query CSV:  $PER_QUERY_CSV"
echo "  total_rgs CSV:  $TOTAL_CSV"
echo "  joined CSV:     $RESULT_CSV"
echo "  raw stderr:     $RAW_DIR/"
echo ""
echo "Quick view:"
head -5 "$RESULT_CSV" | column -t -s,
