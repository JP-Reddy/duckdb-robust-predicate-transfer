#!/bin/bash
# profile_query.sh - Run a JOB or TPCH query with and without Robust, show operator breakdown.
#
# Usage:
#   ./profile_query.sh <query_name>                      # JOB (default), e.g. 1a
#   ./profile_query.sh --workload tpch <query_name>      # TPCH, e.g. 03 or q03
#   ./profile_query.sh --sql "SELECT ..."                # inline SQL
#   ./profile_query.sh --robust-only 1a                     # skip baseline
#   ./profile_query.sh --no-jfp robust 1a                   # disable join_filter_pushdown

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
DUCKDB="$PROJECT_ROOT/build/release/duckdb"
EXT="$PROJECT_ROOT/build/release/extension/robust/robust.duckdb_extension"
PROFILE_PY="$SCRIPT_DIR/profile_breakdown.py"

WORKLOAD="job"
ROBUST_ONLY=false
FORWARD_ONLY=false
QUERY_SQL=""
QUERY_NAME=""
NO_JFP=""
HEURISTIC=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --workload)
            WORKLOAD="$2"
            if [[ "$WORKLOAD" != "job" && "$WORKLOAD" != "tpch" ]]; then
                echo "Error: --workload must be 'job' or 'tpch'"; exit 1
            fi
            shift 2 ;;
        --sql)       QUERY_SQL="$2"; QUERY_NAME="inline"; shift 2 ;;
        --robust-only)  ROBUST_ONLY=true; shift ;;
        --forward-only) FORWARD_ONLY=true; shift ;;
        --no-jfp)
            NO_JFP="$2"
            if [[ "$NO_JFP" != "robust" && "$NO_JFP" != "baseline" && "$NO_JFP" != "both" ]]; then
                echo "Error: --no-jfp must be 'robust', 'baseline', or 'both'"; exit 1
            fi
            shift 2 ;;
        --heuristic)
            HEURISTIC="$2"
            if [[ "$HEURISTIC" != "largest_root" && "$HEURISTIC" != "join_order" && "$HEURISTIC" != "all" ]]; then
                echo "Error: --heuristic must be 'largest_root', 'join_order', or 'all'"; exit 1
            fi
            shift 2 ;;
        -h|--help)
            echo "Usage: $0 [options] <query_name>"
            echo "  <query_name>       JOB (e.g. 1a, 2b) or TPCH (e.g. 3, q03) query name"
            echo "  --workload <w>     'job' (default) or 'tpch'"
            echo "  --sql \"<SQL>\"      run inline SQL instead of a stored query"
            echo "  --robust-only         only profile with Robust (skip baseline)"
            echo "  --forward-only     use forward-only pass mode for Robust"
            echo "  --heuristic <h>    Robust heuristic: largest_root, join_order, or all (compare both)"
            echo "  --no-jfp <target>  disable join_filter_pushdown (robust, baseline, or both)"
            exit 0 ;;
        *)
            if [ -z "$QUERY_NAME" ]; then
                QUERY_NAME="$1"
            else
                echo "Unknown option: $1"; exit 1
            fi
            shift ;;
    esac
done

if [ -z "$QUERY_NAME" ]; then
    echo "Error: provide a query name or --sql. Use -h for help."
    exit 1
fi

# resolve workload-specific paths
if [ "$WORKLOAD" = "tpch" ]; then
    DB="$PROJECT_ROOT/tpchdata/tpch_sf1.duckdb"
    QUERIES_DIR="$PROJECT_ROOT/tpchdata/queries"
    # normalize "3" / "q3" / "q03" -> "q03"
    if [ "$QUERY_NAME" != "inline" ]; then
        raw="${QUERY_NAME#q}"; raw="${raw#0}"
        if [[ "$raw" =~ ^[0-9]+$ ]]; then
            QUERY_NAME=$(printf "q%02d" "$raw")
        fi
    fi
else
    DB="$PROJECT_ROOT/jobdata/imdb.duckdb"
    QUERIES_DIR="$PROJECT_ROOT/jobdata/queries"
fi

# resolve SQL
if [ -z "$QUERY_SQL" ]; then
    QUERY_FILE="$QUERIES_DIR/${QUERY_NAME}.sql"
    [ -f "$QUERY_FILE" ] || { echo "Query file not found: $QUERY_FILE"; exit 1; }
    QUERY_SQL=$(cat "$QUERY_FILE")
fi

for f in "$DUCKDB" "$DB" "$EXT" "$PROFILE_PY"; do
    [ -f "$f" ] || { echo "Missing: $f"; exit 1; }
done

TMP_BASE=$(mktemp /tmp/profile_base_XXXXXX.json)
TMP_ROBUST=$(mktemp /tmp/profile_robust_XXXXXX.json)
TMP_JO=$(mktemp /tmp/profile_jo_XXXXXX.json)
TMP_DB=""
trap 'rm -f $TMP_BASE $TMP_ROBUST $TMP_JO; [ -n "$TMP_DB" ] && rm -f "$TMP_DB"' EXIT

# if the db is locked, copy it to a temp file
ACTUAL_DB="$DB"
if ! "$DUCKDB" "$DB" -unsigned -c "SELECT 1;" > /dev/null 2>&1; then
    TMP_DB=$(mktemp /tmp/job_profile_XXXXXX.duckdb)
    cp "$DB" "$TMP_DB"
    ACTUAL_DB="$TMP_DB"
    echo "(database locked, using temp copy)"
fi

JFP_DISABLE="SET disabled_optimizers = 'join_filter_pushdown';"
JFP_ROBUST=""
JFP_BASE=""
if [[ "$NO_JFP" = "robust" || "$NO_JFP" = "both" ]]; then JFP_ROBUST="$JFP_DISABLE"; fi
if [[ "$NO_JFP" = "baseline" || "$NO_JFP" = "both" ]]; then JFP_BASE="$JFP_DISABLE"; fi

ROBUST_PASS_MODE=""
if [ "$FORWARD_ONLY" = "true" ]; then
    ROBUST_PASS_MODE="SET robust_pass_mode = 'forward_only';"
fi

ROBUST_HEURISTIC=""
if [[ -n "$HEURISTIC" && "$HEURISTIC" != "all" ]]; then
    ROBUST_HEURISTIC="SET robust_heuristic = '$HEURISTIC';"
fi

echo "Profiling query: $QUERY_NAME"
[ -n "$NO_JFP" ] && echo "(join_filter_pushdown disabled for: $NO_JFP)"
[ "$FORWARD_ONLY" = "true" ] && echo "(forward-only pass mode)"
[ -n "$HEURISTIC" ] && echo "(heuristic: $HEURISTIC)"
echo ""

run_profile() {
    local label="$1"
    local outfile="$2"
    local extra_settings="$3"
    local with_ext="$4"
    echo -n "Running $label... "
    if [ "$with_ext" = "true" ]; then
        "$DUCKDB" "$ACTUAL_DB" -unsigned -c "
$JFP_ROBUST
PRAGMA enable_profiling='json';
PRAGMA profiling_output='$outfile';
LOAD '$EXT';
$extra_settings
$QUERY_SQL
" > /dev/null 2>/dev/null
    else
        "$DUCKDB" "$ACTUAL_DB" -unsigned -c "
$JFP_BASE
PRAGMA enable_profiling='json';
PRAGMA profiling_output='$outfile';
$extra_settings
$QUERY_SQL
" > /dev/null 2>/dev/null
    fi
    echo "done"
}

if [ "$HEURISTIC" = "all" ]; then
    # 3-way comparison: baseline vs largest_root vs join_order
    run_profile "baseline" "$TMP_BASE" "" "false"
    run_profile "Robust (largest_root)" "$TMP_ROBUST" "$ROBUST_PASS_MODE SET robust_heuristic = 'largest_root';" "true"
    run_profile "Robust (join_order)" "$TMP_JO" "$ROBUST_PASS_MODE SET robust_heuristic = 'join_order';" "true"
    python3 "$PROFILE_PY" "$TMP_BASE" "$TMP_ROBUST" "$TMP_JO"
elif [ "$ROBUST_ONLY" = "true" ]; then
    run_profile "Robust" "$TMP_ROBUST" "$ROBUST_PASS_MODE $ROBUST_HEURISTIC" "true"
    python3 "$PROFILE_PY" "$TMP_ROBUST"
else
    run_profile "baseline" "$TMP_BASE" "" "false"
    run_profile "Robust" "$TMP_ROBUST" "$ROBUST_PASS_MODE $ROBUST_HEURISTIC" "true"
    python3 "$PROFILE_PY" "$TMP_BASE" "$TMP_ROBUST"
fi
