#!/bin/bash
# vendor_duckdb_bench.sh - Materialize Robust-owned content into the DuckDB submodule.
#
# Run by `make release` / `make debug` automatically. Safe to re-run.
#
# What it does:
#   1. Copies bench_suites/{imdb_robust,imdb_robust_fwd,imdb_robust_jo,tpch_baseline,tpch_robust}/
#      into duckdb/benchmark/  (so benchmark_runner can find them)
#   2. Applies patches/*.patch to the duckdb submodule (idempotent: skips if already applied)
#   3. Creates a portable relative symlink  benchmark -> duckdb/benchmark  at the project root
#      (so scripts/bench_*.sh can reference benchmark/<suite>/<query>.benchmark from PROJECT_ROOT)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

SUITE_SRC="$PROJECT_ROOT/bench_suites"
DUCKDB_BENCH="$PROJECT_ROOT/duckdb/benchmark"
PATCH_DIR="$PROJECT_ROOT/patches"

# 1. copy vendored suites into duckdb/benchmark/
if [ -d "$SUITE_SRC" ]; then
    for suite_path in "$SUITE_SRC"/*/; do
        suite_name="$(basename "$suite_path")"
        # rsync would be nicer but we want zero deps; cp -R is enough.
        rm -rf "$DUCKDB_BENCH/$suite_name"
        cp -R "$suite_path" "$DUCKDB_BENCH/$suite_name"
    done
    echo "  bench suites copied: $(ls -1 "$SUITE_SRC" | tr '\n' ' ')"
else
    echo "  (no $SUITE_SRC dir; skipping suite copy)"
fi

# 2. apply patches idempotently
apply_patch() {
    local patch_path="$1"
    local name; name="$(basename "$patch_path")"
    if git -C "$PROJECT_ROOT/duckdb" apply -R --check "$patch_path" >/dev/null 2>&1; then
        echo "  $name: already applied"
    elif git -C "$PROJECT_ROOT/duckdb" apply --check "$patch_path" >/dev/null 2>&1; then
        git -C "$PROJECT_ROOT/duckdb" apply "$patch_path"
        echo "  $name: applied"
    else
        echo "  $name: ERROR - does not apply cleanly to current duckdb submodule" >&2
        echo "    (probably means upstream DuckDB changed the patched region; the patch needs refreshing)" >&2
        exit 1
    fi
}

if [ -d "$PATCH_DIR" ]; then
    for patch_path in "$PATCH_DIR"/*.patch; do
        [ -e "$patch_path" ] || continue
        apply_patch "$patch_path"
    done
fi

# 3. ensure portable relative symlink benchmark -> duckdb/benchmark
SYMLINK="$PROJECT_ROOT/benchmark"
if [ -L "$SYMLINK" ]; then
    current_target="$(readlink "$SYMLINK")"
    if [ "$current_target" != "duckdb/benchmark" ]; then
        rm "$SYMLINK"
        ln -s duckdb/benchmark "$SYMLINK"
        echo "  benchmark symlink: repointed to duckdb/benchmark (was $current_target)"
    fi
elif [ -e "$SYMLINK" ]; then
    echo "  WARNING: $SYMLINK exists and is not a symlink; leaving untouched" >&2
else
    ln -s duckdb/benchmark "$SYMLINK"
    echo "  benchmark symlink: created -> duckdb/benchmark"
fi
