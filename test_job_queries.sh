#!/bin/bash
# test_job_queries.sh - Test all JOB queries with and without RPT extension
# 
# Usage: ./test_job_queries.sh [options]
#   --generate-baseline    Generate baseline results only (no comparison)
#   --test-only           Run tests against existing baseline (skip baseline generation)
#   --query <name>        Test a specific query (e.g., --query 1a)
#   --verbose             Show detailed output for failures
#   --timing              Show timing information
#   --runs N              Run each query N times and take the minimum (default: 1)
#   --limit N             Only run the first N queries (default: all)

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB="$SCRIPT_DIR/build/release/duckdb"
DB="$SCRIPT_DIR/jobdata/job.duckdb"
EXT="$SCRIPT_DIR/build/release/extension/rpt/rpt.duckdb_extension"
QUERIES_DIR="$SCRIPT_DIR/jobdata/queries_updated"
RESULTS_DIR="$SCRIPT_DIR/job_test_results"

# Options
GENERATE_BASELINE=false
TEST_ONLY=false
SPECIFIC_QUERY=""
VERBOSE=false
TIMING=false
RUNS=1
LIMIT=0

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --generate-baseline)
            GENERATE_BASELINE=true
            shift
            ;;
        --test-only)
            TEST_ONLY=true
            shift
            ;;
        --query)
            SPECIFIC_QUERY="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --timing)
            TIMING=true
            shift
            ;;
        --runs)
            RUNS="$2"
            shift 2
            ;;
        --limit)
            LIMIT="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Check prerequisites
check_prerequisites() {
    if [ ! -f "$DUCKDB" ]; then
        echo -e "${RED}Error: DuckDB binary not found at $DUCKDB${NC}"
        echo "Run 'GEN=ninja make release' first"
        exit 1
    fi

    if [ ! -f "$DB" ]; then
        echo -e "${RED}Error: JOB database not found at $DB${NC}"
        echo "Run: $DUCKDB $DB -unsigned -f jobdata/load_job.sql"
        exit 1
    fi

    if [ ! -f "$EXT" ]; then
        echo -e "${RED}Error: RPT extension not found at $EXT${NC}"
        echo "Run 'GEN=ninja make release' first"
        exit 1
    fi

    if [ ! -d "$QUERIES_DIR" ]; then
        echo -e "${RED}Error: Queries directory not found at $QUERIES_DIR${NC}"
        exit 1
    fi
}

# Run a query and capture output
run_query() {
    local query="$1"
    local with_extension="$2"
    local output_file="$3"
    
    if [ "$with_extension" = "true" ]; then
        # Run with extension, filter debug output
        "$DUCKDB" "$DB" -unsigned -noheader -list -c "LOAD '$EXT'; $query" 2>/dev/null > "$output_file" || true
    else
        # Run without extension (baseline)
        "$DUCKDB" "$DB" -unsigned -noheader -list -c "$query" 2>/dev/null > "$output_file" || true
    fi
}

# Run a query with timing using DuckDB's built-in profiling (runs $RUNS times, returns minimum)
run_query_timed() {
    local query="$1"
    local with_extension="$2"
    local output_file="$3"

    local prof_dir=$(mktemp -d /tmp/prof_XXXXXXXX)
    local prof_file="$prof_dir/profile.json"
    local min_time=""

    for ((r=1; r<=RUNS; r++)); do
        if [ "$with_extension" = "true" ]; then
            "$DUCKDB" "$DB" -unsigned -noheader -list -c "
LOAD '$EXT';
PRAGMA enable_profiling='json';
PRAGMA profiling_output='$prof_file';
$query" 2>/dev/null > "$output_file" || true
        else
            "$DUCKDB" "$DB" -unsigned -noheader -list -c "
PRAGMA enable_profiling='json';
PRAGMA profiling_output='$prof_file';
$query" 2>/dev/null > "$output_file" || true
        fi

        # extract query execution time from profiling JSON
        local elapsed=$(python3 -c "
import json, sys
try:
    with open('$prof_file') as f:
        d = json.load(f)
    print(d.get('latency', 0))
except:
    print(0)
")
        if [ -z "$min_time" ] || [ "$(python3 -c "print(1 if $elapsed < $min_time else 0)")" = "1" ]; then
            min_time="$elapsed"
        fi
    done

    rm -rf "$prof_dir"
    echo "scale=3; $min_time / 1" | bc
}

# Generate baseline results for all queries
generate_baseline() {
    echo "=== Generating Baseline Results ==="
    mkdir -p "$RESULTS_DIR/baseline"
    
    local count=0
    local total=$(ls -1 "$QUERIES_DIR"/*.sql 2>/dev/null | wc -l | tr -d ' ')
    
    for query_file in "$QUERIES_DIR"/*.sql; do
        local query_name=$(basename "$query_file" .sql)
        ((count++))
        
        echo -n "[$count/$total] Generating baseline for $query_name... "
        
        local query=$(cat "$query_file")
        run_query "$query" "false" "$RESULTS_DIR/baseline/$query_name.txt"
        
        echo -e "${GREEN}done${NC}"
    done
    
    echo ""
    echo "Baseline results saved to $RESULTS_DIR/baseline/"
}

# Test a single query
test_query() {
    local query_name="$1"
    local query_file="$QUERIES_DIR/$query_name.sql"
    
    if [ ! -f "$query_file" ]; then
        echo -e "${RED}Query file not found: $query_file${NC}"
        return 1
    fi
    
    local query=$(cat "$query_file")
    local baseline_file="$RESULTS_DIR/baseline/$query_name.txt"
    local rpt_file="$RESULTS_DIR/rpt/$query_name.txt"
    
    mkdir -p "$RESULTS_DIR/rpt"
    
    mkdir -p "$RESULTS_DIR/baseline"

    if [ "$TIMING" = "true" ]; then
        local baseline_time=$(run_query_timed "$query" "false" "$baseline_file")
        local rpt_time=$(run_query_timed "$query" "true" "$rpt_file")
    else
        run_query "$query" "false" "$baseline_file"
        run_query "$query" "true" "$rpt_file"
    fi
    
    # Compare results
    if diff -q "$baseline_file" "$rpt_file" > /dev/null 2>&1; then
        if [ "$TIMING" = "true" ]; then
            local speedup=$(echo "scale=2; $baseline_time / $rpt_time" | bc 2>/dev/null || echo "N/A")
            if [ "$RUNS" -gt 1 ]; then
                echo -e "${GREEN}✅ PASS${NC} (baseline: ${baseline_time}s [min of $RUNS], rpt: ${rpt_time}s [min of $RUNS], speedup: ${speedup}x)"
            else
                echo -e "${GREEN}✅ PASS${NC} (baseline: ${baseline_time}s, rpt: ${rpt_time}s, speedup: ${speedup}x)"
            fi

            # accumulate log-speedup for geometric mean
            local log_sp=$(python3 -c "import math; print(math.log($baseline_time / $rpt_time))" 2>/dev/null || echo "0")
            LOG_SUM_SPEEDUP=$(python3 -c "print($LOG_SUM_SPEEDUP + $log_sp)")
            ((TIMED_QUERY_COUNT++))

            # track faster/slower counts (use 5% threshold to avoid noise)
            local is_faster=$(echo "$baseline_time > $rpt_time * 1.05" | bc 2>/dev/null || echo "0")
            local is_slower=$(echo "$rpt_time > $baseline_time * 1.05" | bc 2>/dev/null || echo "0")

            if [ "$is_faster" = "1" ]; then
                ((RPT_FASTER++))
                FASTER_QUERIES="$FASTER_QUERIES $query_name"
            elif [ "$is_slower" = "1" ]; then
                ((RPT_SLOWER++))
                SLOWER_QUERIES="$SLOWER_QUERIES $query_name"
            else
                ((RPT_SAME++))
            fi
        else
            echo -e "${GREEN}✅ PASS${NC}"
        fi
        return 0
    else
        echo -e "${RED}❌ FAIL${NC}"
        
        if [ "$VERBOSE" = "true" ]; then
            echo "  Expected (first 10 lines):"
            head -10 "$baseline_file" | sed 's/^/    /'
            echo "  Got (first 10 lines):"
            head -10 "$rpt_file" | sed 's/^/    /'
            echo "  Diff:"
            diff "$baseline_file" "$rpt_file" | head -20 | sed 's/^/    /'
        fi
        return 1
    fi
}

# Global timing counters (for tracking RPT performance)
RPT_FASTER=0
RPT_SLOWER=0
RPT_SAME=0
FASTER_QUERIES=""
SLOWER_QUERIES=""
LOG_SUM_SPEEDUP="0"
TIMED_QUERY_COUNT=0

# Test all queries
test_all_queries() {
    echo "=== Testing All JOB Queries ==="
    echo ""
    
    mkdir -p "$RESULTS_DIR/rpt"
    
    local passed=0
    local failed=0
    local failed_queries=""
    local count=0
    local total=$(ls -1 "$QUERIES_DIR"/*.sql 2>/dev/null | wc -l | tr -d ' ')
    
    # Reset timing counters
    RPT_FASTER=0
    RPT_SLOWER=0
    RPT_SAME=0
    FASTER_QUERIES=""
    SLOWER_QUERIES=""
    LOG_SUM_SPEEDUP="0"
    TIMED_QUERY_COUNT=0
    
    # Sort queries naturally (1a, 1b, 1c, 2a, ... not 1a, 10a, 11a, ...)
    for query_file in $(ls -1 "$QUERIES_DIR"/*.sql | sort -V); do
        local query_name=$(basename "$query_file" .sql)
        ((count++))

        if [ "$LIMIT" -gt 0 ] && [ "$count" -gt "$LIMIT" ]; then
            break
        fi

        echo -n "[$count/$total] Testing $query_name... "
        
        if test_query "$query_name"; then
            ((passed++))
        else
            ((failed++))
            failed_queries="$failed_queries $query_name"
        fi
    done
    
    # Summary
    echo ""
    echo "=========================================="
    echo "               SUMMARY"
    echo "=========================================="
    echo -e "Passed: ${GREEN}$passed${NC} / $total"
    echo -e "Failed: ${RED}$failed${NC} / $total"
    
    if [ -n "$failed_queries" ]; then
        echo ""
        echo -e "${RED}Failed queries:${NC}"
        for q in $failed_queries; do
            echo "  - $q"
        done
        echo ""
        echo "Run with --verbose to see details, or test individual queries:"
        echo "  ./test_job_queries.sh --query <name> --verbose"
    fi
    
    # Timing summary (only when --timing is enabled)
    if [ "$TIMING" = "true" ]; then
        echo ""
        echo "=========================================="
        echo "           TIMING SUMMARY"
        echo "=========================================="
        echo -e "RPT Faster: ${GREEN}$RPT_FASTER${NC} queries"
        echo -e "RPT Slower: ${RED}$RPT_SLOWER${NC} queries"
        echo -e "RPT Same:   $RPT_SAME queries"

        if [ "$TIMED_QUERY_COUNT" -gt 0 ]; then
            local geo_mean=$(python3 -c "import math; print(f'{math.exp($LOG_SUM_SPEEDUP / $TIMED_QUERY_COUNT):.3f}')")
            echo ""
            echo -e "Geometric Mean Speedup: ${YELLOW}${geo_mean}x${NC} (over $TIMED_QUERY_COUNT queries)"
        fi

        if [ -n "$FASTER_QUERIES" ]; then
            echo ""
            echo -e "${GREEN}Queries where RPT was faster:${NC}"
            for q in $FASTER_QUERIES; do
                echo "  - $q"
            done
        fi

        if [ -n "$SLOWER_QUERIES" ]; then
            echo ""
            echo -e "${RED}Queries where RPT was slower:${NC}"
            for q in $SLOWER_QUERIES; do
                echo "  - $q"
            done
        fi
    fi
    
    # Save summary to file
    {
        echo "Test run: $(date)"
        echo "Passed: $passed / $total"
        echo "Failed: $failed / $total"
        echo "Failed queries:$failed_queries"
        if [ "$TIMING" = "true" ]; then
            echo ""
            echo "Timing Summary:"
            echo "RPT Faster: $RPT_FASTER queries"
            echo "RPT Slower: $RPT_SLOWER queries"
            echo "RPT Same: $RPT_SAME queries"
            echo "Faster queries:$FASTER_QUERIES"
            echo "Slower queries:$SLOWER_QUERIES"
            if [ "$TIMED_QUERY_COUNT" -gt 0 ]; then
                local geo_mean=$(python3 -c "import math; print(f'{math.exp($LOG_SUM_SPEEDUP / $TIMED_QUERY_COUNT):.3f}')")
                echo "Geometric Mean Speedup: ${geo_mean}x (over $TIMED_QUERY_COUNT queries)"
            fi
        fi
    } > "$RESULTS_DIR/summary.txt"
    
    if [ $failed -gt 0 ]; then
        return 1
    fi
    return 0
}

# Main
main() {
    echo "JOB Query Test Suite for RPT Extension"
    echo "======================================="
    echo ""
    
    check_prerequisites
    
    if [ -n "$SPECIFIC_QUERY" ]; then
        echo "Testing query: $SPECIFIC_QUERY"
        echo ""
        test_query "$SPECIFIC_QUERY"
    elif [ "$GENERATE_BASELINE" = "true" ]; then
        generate_baseline
    elif [ "$TEST_ONLY" = "true" ]; then
        if [ ! -d "$RESULTS_DIR/baseline" ]; then
            echo -e "${RED}Error: No baseline results found. Run with --generate-baseline first${NC}"
            exit 1
        fi
        test_all_queries
    else
        # Default: generate baseline if needed, then test
        if [ ! -d "$RESULTS_DIR/baseline" ] || [ -z "$(ls -A "$RESULTS_DIR/baseline" 2>/dev/null)" ]; then
            generate_baseline
            echo ""
        fi
        test_all_queries
    fi
}

main "$@"

