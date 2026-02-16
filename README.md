# Robust

A DuckDB extension for robust query optimization through predicate transfer. Built on DuckDB v1.4.4.

## Overview

Robust analyzes the join graph of multi-join queries and propagates bloom filters across joins to eliminate unnecessary rows before they ever reach a join operator. In complex queries with many joins, the vast majority of intermediate rows may not survive to the final result. Predicate transfer filters these rows out early, so only a small fraction of the data actually participates in each join.

We invite researchers and practitioners to experiment with this extension to evaluate the performance impact of predicate transfer on real workloads.

### How Predicate Transfer Works

In a multi-join query, tables are joined in sequence. Without predicate transfer, each table's full scan feeds into the join pipeline, even though most rows will be discarded by downstream joins. This wastes I/O, memory, and computation on rows that contribute nothing to the final result.

Predicate transfer addresses this by propagating bloom filters across the entire join graph:

1. **Build bloom filters** from the join keys of smaller tables.
2. **Apply bloom filters** to larger tables *before* they reach the join, filtering out rows whose keys have no match on the other side.
3. **Cascade across joins** -- a bloom filter built from one join can filter the input to another join further in the plan, progressively reducing intermediate result sizes.

The optimizer walks the join graph and inserts `CREATE_BF` (build) and `USE_BF` (probe) operators into the physical plan. This is transparent to the user -- queries produce the same results, just faster because only the rows that can actually contribute to join matches flow through the pipeline.

### Bloom Filter Implementation

The bloom filter used in this extension is ported from [Apache Arrow Acero's `BlockedBloomFilter`](https://github.com/apache/arrow/blob/main/cpp/src/arrow/acero/bloom_filter.h) (Arrow 21+). All Arrow dependencies have been replaced with DuckDB/stdlib equivalents to produce a self-contained, header-only implementation with no external dependencies. Key properties:

- **1 cache line access per key** -- single 64-bit block lookup
- **Pre-generated mask table** -- 1024 masks (57-bit, 4-5 bits set) eliminate per-query mask computation
- **Memory prefetching** for large filters (>256KB) -- 16 iterations ahead
- **Folding** for sparse filters -- after building, compresses by OR-ing halves until >25% density, reducing memory footprint for selective joins
- **8 bits per key** -- compact and cache-friendly
- **Atomic inserts** (`fetch_or`) for thread-safe parallel building

## Building

### Prerequisites

```bash
git clone https://github.com/Microsoft/vcpkg.git
./vcpkg/bootstrap-vcpkg.sh
export VCPKG_TOOLCHAIN_PATH=`pwd`/vcpkg/scripts/buildsystems/vcpkg.cmake
```

### Build

```bash
# release build
GEN=ninja make release

# debug build (includes AddressSanitizer)
GEN=ninja make debug
```

Build artifacts:
- `./build/release/duckdb` -- DuckDB shell
- `./build/release/extension/rpt/rpt.duckdb_extension` -- loadable extension

## Running

The extension is not yet published to the DuckDB extension repository, so it must be loaded as an unsigned extension:

```bash
./build/release/duckdb -unsigned
```

```sql
LOAD 'build/release/extension/rpt/rpt.duckdb_extension';

-- create test tables
CREATE TEMP TABLE t1 AS SELECT i AS id FROM range(100000) tbl(i);
CREATE TEMP TABLE t2 AS SELECT i AS id FROM range(50000) tbl(i);

-- verify correctness
SELECT count(*) FROM t1 JOIN t2 ON t1.id = t2.id;
-- returns 50000

-- inspect the optimized plan (look for CREATE_BF and USE_BF operators)
EXPLAIN SELECT * FROM t1 JOIN t2 ON t1.id = t2.id;
```

## Benchmarks (JOB)

The repository includes a benchmark script that runs all 113 [Join Order Benchmark](http://www.vldb.org/pvldb/vol9/p204-leis.pdf) queries with and without the extension, compares result correctness, and reports per-query timing and geometric mean speedup.

### Setup

1. Build the extension (see above).
2. Load the JOB dataset into `jobdata/job.duckdb` (the script expects this path).

### Running

```bash
# correctness only
./test_job_queries.sh

# with timing (single run per query)
./test_job_queries.sh --timing

# with timing (minimum of 3 runs per query, for more stable numbers)
./test_job_queries.sh --timing --runs 3

# test a specific query
./test_job_queries.sh --query 1a --timing

# test first N queries
./test_job_queries.sh --timing --limit 10
```

The script generates a baseline (without the extension) on first run, then compares results and timing against the extension-enabled run. A summary is saved to `job_test_results/summary.txt`.

## Current Status

- Predicate transfer working for multi-join queries with equality join conditions
- Parallel bloom filter build/probe integrated into DuckDB's pipeline execution
- Correctness verified on the full JOB benchmark (113 queries)

## License

This project is based on the [DuckDB Extension Template](https://github.com/duckdb/extension-template). The bloom filter implementation is ported from [Apache Arrow](https://github.com/apache/arrow) (Apache License 2.0).