#!/usr/bin/env bash
#
# setup_imdb_data.sh — one-time setup for the IMDB / Join Order Benchmark data.
#
# Materializes jobdata/imdb.duckdb by streaming the 21 IMDB tables from DuckDB's
# public release artifacts (github.com/duckdb/duckdb-data, ~2.6G download). The
# SQL is the same load.sql DuckDB's own benchmark_runner uses, taken from the
# duckdb submodule at duckdb/benchmark/imdb/init/load.sql.
#
# Usage:
#   ./scripts/setup_imdb_data.sh           # skips if jobdata/imdb.duckdb exists
#   ./scripts/setup_imdb_data.sh --force   # rebuild even if it exists
#
# Environment:
#   DUCKDB    path to duckdb CLI (default: ./build/release/duckdb)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

DB="$PROJECT_ROOT/jobdata/imdb.duckdb"
LOAD_SQL="$PROJECT_ROOT/duckdb/benchmark/imdb/init/load.sql"
DUCKDB="${DUCKDB:-$PROJECT_ROOT/build/release/duckdb}"

FORCE=0
for arg in "$@"; do
    case "$arg" in
        --force|-f) FORCE=1 ;;
        --help|-h)
            sed -n '3,15p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "unknown arg: $arg (use --help)" >&2
            exit 2
            ;;
    esac
done

if [ ! -f "$DUCKDB" ]; then
    echo "error: duckdb CLI not found at $DUCKDB" >&2
    echo "  build it with 'GEN=ninja make release', or set DUCKDB=/path/to/duckdb" >&2
    exit 1
fi

if [ ! -f "$LOAD_SQL" ]; then
    echo "error: load.sql not found at $LOAD_SQL" >&2
    echo "  did you 'git submodule update --init --recursive'?" >&2
    exit 1
fi

if [ -f "$DB" ] && [ "$FORCE" -eq 0 ]; then
    echo "$DB already exists; nothing to do (pass --force to rebuild)."
    exit 0
fi

mkdir -p "$(dirname "$DB")"
rm -f "$DB"

echo "Building $DB"
echo "  source : $LOAD_SQL"
echo "  fetches: 21 parquet files from github.com/duckdb/duckdb-data (~2.6G)"
echo

{
    echo ".bail on"
    echo "INSTALL httpfs; LOAD httpfs;"
    cat "$LOAD_SQL"
} | "$DUCKDB" "$DB"

echo
echo "Done. Loaded $DB"
