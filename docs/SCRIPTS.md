# Scripts

## Profiling & Analysis

### profile_query.sh
Profiles a single query with and without RPT. Shows operator tree, scan detail, and a comparison summary with CPU breakdown and per-table scan stats.

```
./profile_query.sh <query>           # e.g. ./profile_query.sh 1a
./profile_query.sh --sql "SELECT…"   # inline SQL
./profile_query.sh --rpt-only 1a     # skip baseline
./profile_query.sh --no-jfp rpt 1a   # disable join_filter_pushdown for rpt/baseline/both
```

### warmup_bench.sh
Runs a query N times in a single session (1 cold + N-1 warm) for both baseline and RPT. Outputs a CSV and a wall/CPU time plot.

```
./warmup_bench.sh 32a        # 5 runs (default)
./warmup_bench.sh -n 10 32a  # 10 runs
./warmup_bench.sh --sql "…"  # inline SQL
```

| Flag | Description |
|------|-------------|
| `-n <N>` | Number of iterations (default: 5) |
| `--sql "…"` | Inline SQL instead of query name |

Output: `profiling_results/<query>_warmup.csv`, `profiling_results/<query>_warmup.png`

### scan_compare.sh
Runs all JOB queries (N runs each, single session per mode), ranks by rows scanned / wall time / CPU time % increase (RPT vs baseline). Prints per-query stats live.

```
./scan_compare.sh                  # top 10 by scanned %, 5 runs
./scan_compare.sh -n 20            # top 20
./scan_compare.sh -r 3             # 3 runs per query
./scan_compare.sh --sort wall      # sort by wall time % increase
./scan_compare.sh --sort cpu       # sort by CPU time % increase
```

| Flag | Description |
|------|-------------|
| `-n <N>` | Show top N queries (default: 10) |
| `-r <R>` | Runs per query (default: 5) |
| `--sort <key>` | `scanned` (default), `wall`, `cpu` |

Output: `profiling_results/scan_compare.csv`

### time_breakdown.sh
Runs all JOB queries, breaks down CPU time by operator category (SEQ_SCAN, HASH_JOIN, BF, Other) with percentages. Two lines per query (BASE + RPT).

```
./time_breakdown.sh                # all queries, 5 runs
./time_breakdown.sh -r 3           # 3 runs
./time_breakdown.sh -q 1a,2b,13a   # specific queries only
```

| Flag | Description |
|------|-------------|
| `-r <R>` | Runs per query (default: 5) |
| `-q <list>` | Comma-separated query names (default: all) |

Output: `profiling_results/time_breakdown.csv`

## Benchmarking

### bench_job.sh
Benchmarks JOB queries with RPT, tracking wall time and RPT-specific profiling stats (sink, source, finalize, probe times, rows filtered). Appends to a summary CSV for tracking across commits.

```
./bench_job.sh                          # all queries, min of 3 runs
./bench_job.sh --query 3a --runs 5      # single query, 5 runs
./bench_job.sh --with-baseline          # include baseline comparison
./bench_job.sh --agg median --limit 20  # median of runs, first 20 queries
./bench_job.sh --no-save                # print only, no CSV output
```

| Flag | Description |
|------|-------------|
| `--runs N` | Runs per query (default: 3) |
| `--agg <mode>` | `min` (default), `median`, `max` |
| `--limit N` | Only first N queries |
| `--query <name>` | Single query |
| `--title <text>` | Custom title (default: commit subject) |
| `--with-baseline` | Also run without RPT for comparison |
| `--no-save` | Don't write CSV files |

Output: `benchmark_results/<commit>_<title>_detail.csv`, `benchmark_results/summary.csv`

### bench_commits.sh
Runs `bench_job.sh` across recent git commits. Checks out each commit, rebuilds, and benchmarks. Useful for tracking performance regressions.

```
./bench_commits.sh                          # last 10 commits, query 1a
./bench_commits.sh --query 3a --last 5      # last 5 commits, query 3a
./bench_commits.sh --upto abc123 --runs 5   # from abc123 to HEAD
./bench_commits.sh --reverse                # newest first
```

| Flag | Description |
|------|-------------|
| `--last N` | Last N commits (default: 10) |
| `--upto <commit>` | From commit to HEAD |
| `--query <name>` | Query to benchmark (default: 1a) |
| `--runs N` | Runs per query (default: 3) |
| `--agg <mode>` | `min` (default), `median`, `max` |
| `--reverse` | Run newest to oldest |

### bench_compare.sh
Compares JOB performance between current and previous commit. Builds both, runs `scripts/test_job_queries.sh --timing` 3 times each, reports average geometric mean speedup.

```
./bench_compare.sh
```

### run_bench_compare.sh
Runs DuckDB's built-in benchmark runner for baseline and RPT benchmark suites, then compares results side-by-side with speedup calculations.

```
./run_bench_compare.sh                       # all queries
./run_bench_compare.sh --pattern "03.*"      # specific pattern
./run_bench_compare.sh --rpt-only            # RPT benchmarks only
./run_bench_compare.sh --no-run              # compare existing results
```

| Flag | Description |
|------|-------------|
| `--pattern <pat>` | Query name regex (default: all) |
| `--baseline-only` | Run only baseline |
| `--rpt-only` | Run only RPT |
| `--no-run` | Skip running, compare existing results |
| `--out <dir>` | Output directory (default: `benchmark_results`) |

Requires: `BUILD_BENCHMARK=1 GEN=ninja make release`

## Testing

### scripts/test_job_queries.sh
Tests all JOB queries for correctness (baseline vs RPT result comparison) with optional timing and speedup reporting.

```
./scripts/test_job_queries.sh                           # test all queries
./scripts/test_job_queries.sh --timing --runs 3         # with timing, min of 3
./scripts/test_job_queries.sh --query 1a --verbose      # single query, verbose
./scripts/test_job_queries.sh --generate-baseline       # regenerate baselines
./scripts/test_job_queries.sh --no-jfp both --limit 10  # disable JFP, first 10
```

| Flag | Description |
|------|-------------|
| `--timing` | Show wall times and speedup |
| `--runs N` | Runs per query, take min (default: 1) |
| `--query <name>` | Test single query |
| `--verbose` | Show diff details on failure |
| `--generate-baseline` | Generate baseline results only |
| `--test-only` | Test against existing baselines |
| `--limit N` | First N queries only |
| `--no-jfp <target>` | Disable join_filter_pushdown: `rpt`, `baseline`, `both` |

Output: `job_test_results/`

### test_job3a.sh
Quick smoke test — runs JOB query 3a with and without RPT, compares results.

```
./test_job3a.sh
```

## Other

### debug_duckdb.sh
Runs the debug build of DuckDB with `src/tests.sql`.

```
./debug_duckdb.sh
```

### run_benchmark.sh
Compiles and runs the bloom filter microbenchmark (`src/benchmark/bloom_filter_benchmark.cpp`).

```
./run_benchmark.sh           # full benchmark
./run_benchmark.sh -q        # quick mode
./run_benchmark.sh -v        # verbose
```

### run_threshold_experiment.sh
Compiles and runs the threshold experiment to find the optimal crossover point between single-threaded vs parallel bloom filter building.

```
./run_threshold_experiment.sh              # 8 threads (default)
./run_threshold_experiment.sh -t 4         # 4 threads
./run_threshold_experiment.sh -o out.csv   # custom output file
```

| Flag | Description |
|------|-------------|
| `-t <N>` | Number of threads (default: 8) |
| `-o <file>` | Output CSV (default: `threshold_results.csv`) |
