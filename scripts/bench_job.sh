#!/bin/bash
# bench_job.sh - Run baseline and Robust JOB benchmarks via DuckDB's benchmark_runner, report side-by-side comparison
#
# Usage: ./scripts/bench_job.sh [options]
#   --pattern <pat>   Query name pattern, e.g. "03.*" (default: .* = all)
#   --baseline-only   Run only baseline benchmarks
#   --robust-only        Run only Robust benchmarks
#   --no-run          Skip running, just compare existing results
#   --forward-only    Use forward-only pass mode for Robust
#   --heuristic <name> Robust heuristic: join_order (default), largest_root
#   --out <dir>       Output directory (default: benchmark_results)
#   --metric <m>      Aggregation metric across runs: min (default), geomean
#   --robust-first    Run Robust suite before baseline (default: baseline first)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
RUNNER="$PROJECT_ROOT/build/release/benchmark/benchmark_runner"
PATTERN=".*"
RUN_BASELINE=true
RUN_ROBUST=true
FORWARD_ONLY=false
HEURISTIC=""
OUT_DIR="$PROJECT_ROOT/results/benchmark_results"
METRIC="min"
ROBUST_FIRST=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --pattern) PATTERN="$2"; shift 2 ;;
        --baseline-only) RUN_ROBUST=false; shift ;;
        --robust-only) RUN_BASELINE=false; shift ;;
        --no-run) RUN_BASELINE=false; RUN_ROBUST=false; shift ;;
        --forward-only) FORWARD_ONLY=true; shift ;;
        --heuristic) HEURISTIC="$2"; shift 2 ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        --metric) METRIC="$2"; shift 2 ;;
        --robust-first) ROBUST_FIRST=true; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

mkdir -p "$OUT_DIR"

if [ ! -f "$RUNNER" ]; then
    echo "Error: benchmark_runner not found at $RUNNER"
    echo "Build with: BUILD_BENCHMARK=1 GEN=ninja make release"
    exit 1
fi

BASELINE_RAW="$OUT_DIR/baseline_raw.tsv"
ROBUST_RAW="$OUT_DIR/robust_raw.tsv"

run_baseline() {
    if [ "$RUN_BASELINE" = true ]; then
        echo "Running baseline benchmarks (pattern: $PATTERN)..."
        "$RUNNER" "benchmark/imdb/$PATTERN" 2>&1 | tee "$BASELINE_RAW"
        echo "Baseline done."
    fi
}

run_robust() {
    if [ "$RUN_ROBUST" = true ]; then
        if [ "$HEURISTIC" = "largest_root" ]; then
            ROBUST_SUITE="imdb_robust_lr"
            echo "Running Robust largest_root benchmarks (pattern: $PATTERN)..."
        elif [ "$FORWARD_ONLY" = true ]; then
            ROBUST_SUITE="imdb_robust_fwd"
            echo "Running Robust forward-only benchmarks (pattern: $PATTERN)..."
        else
            ROBUST_SUITE="imdb_robust"
            echo "Running Robust default (join_order) benchmarks (pattern: $PATTERN)..."
        fi
        "$RUNNER" "benchmark/${ROBUST_SUITE}/$PATTERN" 2>&1 | tee "$ROBUST_RAW"
        echo "Robust done."
    fi
}

if [ "$ROBUST_FIRST" = true ]; then
    run_robust
    run_baseline
else
    run_baseline
    run_robust
fi

if [ ! -f "$BASELINE_RAW" ] || [ ! -f "$ROBUST_RAW" ]; then
    echo "Error: need both baseline and Robust results to compare"
    exit 1
fi

python3 - "$BASELINE_RAW" "$ROBUST_RAW" "$OUT_DIR/comparison.tsv" "$METRIC" <<'PYEOF'
import sys
from collections import defaultdict
import math

def aggregate(ts, metric):
    """Aggregate warm runs (skip first) using the given metric."""
    warm = ts[1:] if len(ts) > 1 else ts
    if metric == "geomean":
        return math.exp(sum(math.log(t) for t in warm) / len(warm))
    return min(warm)  # default: min

def parse_results(path, metric):
    """Parse benchmark runner TSV, return {query_name: aggregated_time}."""
    times = defaultdict(list)
    with open(path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("name"):
                continue
            parts = line.split("\t")
            if len(parts) != 3:
                continue
            name, run, t = parts
            qname = name.split("/")[-1].replace(".benchmark", "")
            times[qname].append(float(t))
    return {qname: aggregate(ts, metric) for qname, ts in times.items()}

metric = sys.argv[4] if len(sys.argv) > 4 else "min"
baseline = parse_results(sys.argv[1], metric)
robust = parse_results(sys.argv[2], metric)
out_path = sys.argv[3]

queries = sorted(set(baseline.keys()) & set(robust.keys()),
                 key=lambda q: (int(''.join(c for c in q if c.isdigit()) or '0'),
                                ''.join(c for c in q if c.isalpha())))

if not queries:
    print("No common queries found between baseline and Robust results.")
    sys.exit(1)

faster = []
slower = []
log_speedups = []

metric_label = "geomean" if metric == "geomean" else "min"
print(f"Metric: {metric_label} of warm runs (excluding first/cold run)\n")
header = f"{'Query':<10} {'Baseline(s)':>12} {'Robust(s)':>12} {'Speedup':>10} {'Status':>8}"
sep = "-" * len(header)

lines = [header, sep]

for q in queries:
    b = baseline[q]
    r = robust[q]
    speedup = b / r if r > 0 else float('inf')
    log_speedups.append(math.log(speedup))

    if speedup > 1.05:
        status = "FASTER"
        faster.append((q, speedup))
    elif speedup < 0.95:
        status = "SLOWER"
        slower.append((q, speedup))
    else:
        status = "~same"

    lines.append(f"{q:<10} {b:>12.6f} {r:>12.6f} {speedup:>9.3f}x {status:>8}")

geo_mean = math.exp(sum(log_speedups) / len(log_speedups)) if log_speedups else 1.0
total_b = sum(baseline[q] for q in queries)
total_r = sum(robust[q] for q in queries)

lines.append(sep)
lines.append(f"{'TOTAL':<10} {total_b:>12.6f} {total_r:>12.6f} {total_b/total_r:>9.3f}x")
lines.append("")
lines.append(f"Queries: {len(queries)}  |  Faster: {len(faster)}  |  Slower: {len(slower)}  |  Same: {len(queries)-len(faster)-len(slower)}")
lines.append(f"Geometric mean speedup: {geo_mean:.3f}x")

if faster:
    lines.append("")
    lines.append("Top Robust wins:")
    for q, s in sorted(faster, key=lambda x: -x[1])[:10]:
        lines.append(f"  {q}: {s:.3f}x")

if slower:
    lines.append("")
    lines.append("Top Robust regressions:")
    for q, s in sorted(slower, key=lambda x: x[1])[:10]:
        lines.append(f"  {q}: {s:.3f}x")

output = "\n".join(lines)
print(output)

with open(out_path, "w") as f:
    f.write("query\tbaseline\trobust\tspeedup\n")
    for q in queries:
        b = baseline[q]
        r = robust[q]
        f.write(f"{q}\t{b:.6f}\t{r:.6f}\t{b/r:.3f}\n")

print(f"\nTSV saved to {out_path}")
PYEOF
