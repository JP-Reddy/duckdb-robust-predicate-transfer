#!/bin/bash
# bench_tpch.sh - Run baseline and Robust TPCH benchmarks via DuckDB's benchmark_runner, report side-by-side comparison.
#
# Suites used:
#   benchmark/tpch_baseline/  (baseline, no extension)
#   benchmark/tpch_robust/       (LOAD robust + jfp disabled)
#
# Only the 9 queries where Robust inserts bloom filters and runs correctly are wired up:
#   Q02, Q03, Q07, Q08, Q10, Q11, Q17, Q18, Q21
# The other queries are deliberately omitted: Q01/Q06 are single-table, Q12/Q13/Q14/Q19
# have only one join (Robust's edges<=1 early-exit), Q04/Q15/Q16/Q22 currently insert no BFs,
# and Q05/Q09/Q20 have cyclic join graphs that crash or miscompare under Robust.
#
# Usage: ./scripts/bench_tpch.sh [options]
#   --pattern <pat>    Query name pattern, e.g. "q03" (default: all wired queries)
#   --baseline-only    Run only baseline benchmarks
#   --robust-only         Run only Robust benchmarks
#   --no-run           Skip running, just compare existing results
#   --out <dir>        Output directory (default: benchmark_results/tpch)
#   --metric <m>       Aggregation metric across runs: min (default), geomean

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"
RUNNER="$PROJECT_ROOT/build/release/benchmark/benchmark_runner"
PATTERN="q(02|03|07|08|10|11|17|18|21)\.benchmark"
RUN_BASELINE=true
RUN_ROBUST=true
OUT_DIR="$PROJECT_ROOT/benchmark_results/tpch"
METRIC="min"
DISABLE_TIMEOUT=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --pattern) PATTERN="$2"; shift 2 ;;
        --baseline-only) RUN_ROBUST=false; shift ;;
        --robust-only) RUN_BASELINE=false; shift ;;
        --no-run) RUN_BASELINE=false; RUN_ROBUST=false; shift ;;
        --out) OUT_DIR="$2"; shift 2 ;;
        --metric) METRIC="$2"; shift 2 ;;
        --disable-timeout) DISABLE_TIMEOUT=true; shift ;;
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

RUNNER_FLAGS=()
if [ "$DISABLE_TIMEOUT" = true ]; then
    RUNNER_FLAGS+=(--disable-timeout)
fi

if [ "$RUN_BASELINE" = true ]; then
    echo "Running TPCH baseline benchmarks (pattern: $PATTERN)..."
    "$RUNNER" "${RUNNER_FLAGS[@]}" "benchmark/tpch_baseline/$PATTERN" 2>&1 | tee "$BASELINE_RAW"
    echo "Baseline done."
fi

if [ "$RUN_ROBUST" = true ]; then
    echo "Running TPCH Robust benchmarks (pattern: $PATTERN)..."
    "$RUNNER" "${RUNNER_FLAGS[@]}" "benchmark/tpch_robust/$PATTERN" 2>&1 | tee "$ROBUST_RAW"
    echo "Robust done."
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
    return min(warm)

def parse_results(path, metric):
    """Parse benchmark runner TSV, return {query_name: aggregated_time or None for timeouts}."""
    times = defaultdict(list)
    timed_out = set()
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
            try:
                times[qname].append(float(t))
            except ValueError:
                # benchmark_runner emits a free-form error string when a run hits
                # the timeout or otherwise fails. mark the query as timed-out.
                timed_out.add(qname)
    out = {}
    for qname, ts in times.items():
        out[qname] = aggregate(ts, metric) if ts else None
    for qname in timed_out:
        if qname not in out or out[qname] is None:
            out[qname] = None
    return out

metric = sys.argv[4] if len(sys.argv) > 4 else "min"
baseline = parse_results(sys.argv[1], metric)
robust = parse_results(sys.argv[2], metric)
out_path = sys.argv[3]

queries = sorted(set(baseline.keys()) | set(robust.keys()),
                 key=lambda q: int(''.join(c for c in q if c.isdigit()) or '0'))

if not queries:
    print("No queries found in either baseline or Robust results.")
    sys.exit(1)

faster = []
slower = []
timeouts = []
log_speedups = []

metric_label = "geomean" if metric == "geomean" else "min"
print(f"Metric: {metric_label} of warm runs (excluding first/cold run)\n")
header = f"{'Query':<10} {'Baseline(s)':>12} {'Robust(s)':>12} {'Speedup':>10} {'Status':>8}"
sep = "-" * len(header)

lines = [header, sep]

for q in queries:
    b = baseline.get(q)
    r = robust.get(q)
    if b is None or r is None:
        b_str = f"{b:.6f}" if b is not None else "TIMEOUT"
        r_str = f"{r:.6f}" if r is not None else "TIMEOUT"
        lines.append(f"{q:<10} {b_str:>12} {r_str:>12} {'-':>10} {'TIMEOUT':>8}")
        timeouts.append(q)
        continue
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

geo_mean = math.exp(sum(log_speedups) / len(log_speedups)) if log_speedups else float('nan')
finished = [q for q in queries if baseline.get(q) is not None and robust.get(q) is not None]
total_b = sum(baseline[q] for q in finished)
total_r = sum(robust[q] for q in finished)

lines.append(sep)
if total_r > 0:
    lines.append(f"{'TOTAL':<10} {total_b:>12.6f} {total_r:>12.6f} {total_b/total_r:>9.3f}x   (excl. timeouts)")
lines.append("")
same = len(finished) - len(faster) - len(slower)
lines.append(f"Queries: {len(queries)}  |  Faster: {len(faster)}  |  Slower: {len(slower)}  |  Same: {same}  |  Timeouts: {len(timeouts)}")
lines.append(f"Geometric mean speedup (over {len(finished)} finished queries): {geo_mean:.3f}x")
if timeouts:
    lines.append(f"Timed-out queries: {', '.join(timeouts)}")

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
        b = baseline.get(q)
        r = robust.get(q)
        b_str = f"{b:.6f}" if b is not None else "TIMEOUT"
        r_str = f"{r:.6f}" if r is not None else "TIMEOUT"
        sp = f"{b/r:.3f}" if (b is not None and r is not None and r > 0) else "-"
        f.write(f"{q}\t{b_str}\t{r_str}\t{sp}\n")

print(f"\nTSV saved to {out_path}")
PYEOF
