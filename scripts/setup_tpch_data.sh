#!/usr/bin/env bash
#
# setup_tpch_data.sh — one-time setup for the TPCH benchmark queries and answers.
#
# Populates tpchdata/queries/ and tpchdata/answers/sf1/ from the canonical
# copies shipped in the pinned duckdb submodule (duckdb/extension/tpch/dbgen/).
# These are the same queries/answers DuckDB's own benchmark_runner uses, so they
# stay version-locked to the submodule SHA. The tpch_sf1.duckdb database itself
# is generated on demand by the benchmark_runner via CALL dbgen(sf=1).
#
# Usage:
#   ./scripts/setup_tpch_data.sh           # skips if tpchdata/queries already populated
#   ./scripts/setup_tpch_data.sh --force   # recopy even if present
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SRC_QUERIES="$PROJECT_ROOT/duckdb/extension/tpch/dbgen/queries"
SRC_ANSWERS="$PROJECT_ROOT/duckdb/extension/tpch/dbgen/answers/sf1"
DST_QUERIES="$PROJECT_ROOT/tpchdata/queries"
DST_ANSWERS="$PROJECT_ROOT/tpchdata/answers/sf1"

FORCE=0
for arg in "$@"; do
    case "$arg" in
        --force|-f) FORCE=1 ;;
        --help|-h)
            sed -n '3,13p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "unknown arg: $arg (use --help)" >&2
            exit 2
            ;;
    esac
done

if [ ! -d "$SRC_QUERIES" ] || [ ! -d "$SRC_ANSWERS" ]; then
    echo "error: TPCH source files not found in the duckdb submodule" >&2
    echo "  expected: $SRC_QUERIES" >&2
    echo "        and: $SRC_ANSWERS" >&2
    echo "  did you 'git submodule update --init --recursive'?" >&2
    exit 1
fi

if [ -d "$DST_QUERIES" ] && [ -n "$(ls -A "$DST_QUERIES"/q*.sql 2>/dev/null)" ] && [ "$FORCE" -eq 0 ]; then
    echo "$DST_QUERIES already populated; nothing to do (pass --force to recopy)."
    exit 0
fi

mkdir -p "$DST_QUERIES" "$DST_ANSWERS"
cp "$SRC_QUERIES"/q*.sql "$DST_QUERIES"/
cp "$SRC_ANSWERS"/q*.csv "$DST_ANSWERS"/

echo "Done."
echo "  queries: $(ls "$DST_QUERIES"/q*.sql | wc -l | tr -d ' ') -> $DST_QUERIES"
echo "  answers: $(ls "$DST_ANSWERS"/q*.csv | wc -l | tr -d ' ') -> $DST_ANSWERS"
