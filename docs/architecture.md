# Robust — Architecture

This document explains what the `robust` DuckDB extension does, how it does it, and where to look in the code for each piece. It assumes you've read [the README](../README.md) and have some DuckDB internals familiarity (logical/physical operators, sinks, optimizer extensions).

If you're trying to decide whether Robust is the right tool for a problem, skim §1. If you're trying to understand the algorithm, read §2–5. If you're modifying the code, read §6 onward.

## Contents

1. [Overview](#1-overview)
2. [The rewrite at a glance](#2-the-rewrite-at-a-glance)
3. [The join graph (DAG)](#3-the-join-graph-dag)
4. [Forward pass](#4-forward-pass)
5. [Backward pass](#5-backward-pass)
6. [Operator internals](#6-operator-internals)
7. [Filter types and the picker](#7-filter-types-and-the-picker)
8. [Edge cases and tricks](#8-edge-cases-and-tricks)

---

## 1. Overview

In a multi-join analytic query, most of the rows scanned at the storage layer will never appear in the final result. They get joined to something on one side, then thrown away by a hash join further up the tree. The work of reading them, decompressing them, building hash tables on them, probing those hash tables, and propagating their column values through intermediate operators is wasted.

**Robust** addresses this by implementing **predicate transfer** — a technique that lifts filter information out of join keys, propagates it across the entire join graph, and pushes it down so that probe-side scans skip rows that can't survive downstream joins. The filters propagated are **bloom filters**, **min/max ranges**, and **IN-lists** today; the filter set is extensible (see §7).

The technique is described in two papers:

- [**Predicate Transfer: Efficient Pre-Filtering on Multi-Join Queries**](https://www.cidrdb.org/cidr2024/papers/p22-yang.pdf) — Yang et al., CIDR 2024. The original formulation; introduces the `largest_root` heuristic used by this extension.
- [**Debunking the Myth of Join Ordering: Toward Robust SQL Analytics**](https://arxiv.org/pdf/2502.15181) — 2025. Follow-up work motivating the broader "robust analytics" framing this extension takes its name from.

Robust runs as a DuckDB optimizer extension. It builds a **predicate transfer DAG** over the query's join graph, performs a **forward pass** to construct filter sources, then a **backward pass** to propagate those filters back across the graph. The resulting filters are installed both as new operators in the physical plan (`CREATE_FILTER` / `PROBE_FILTER`) and — for forward-pass filters — as dynamic scan filters on the underlying `SEQ_SCAN`.

### How it differs from DuckDB's native join_filter_pushdown

DuckDB already does a form of pre-filtering internally via the `join_filter_pushdown` optimizer rule (internal name **JFP**). As each hash join builds its hash table, JFP extracts filters from the join keys (bloom filters, min/max ranges, and IN-lists — the same filter types Robust uses) and pushes them down to the probe-side scan.

JFP's pushed filters also chain forward: in an `A ⋈ B ⋈ C` plan where `C` is the deepest build side, JFP can build a filter at the `C ⋈ B` join, push it into the scan of `B`, and then build a fresh filter at the next join up that propagates further down to `A`. So the "single immediate probe side" framing is too narrow — JFP does cascade forward through linear chains.

What JFP *doesn't* do, and what Robust adds, is:

1. **Bushy plans dilute the forward chain.** JFP's forward chaining assumes a single spine of joins. In bushy plans — where multiple build pipelines run in parallel before merging at a higher join — JFP can't carry a filter from one branch into a sibling branch, because the filter has no join to ride. Robust's DAG sees the whole graph at once and propagates filters across branches that JFP can't.
2. **No backward pass.** JFP only flows filters in the build → probe direction within the join order. If an early join has an intermediate-result explosion that a later, more-selective join would have prevented, JFP can't apply that later filter retroactively. Robust's backward pass does exactly that — filters discovered late in the plan get propagated back to scans early in the plan, shrinking intermediate results before they explode.

The two systems are complementary. On baseline measurements we leave JFP on; on Robust runs we typically disable JFP to isolate Robust's contribution. The README's [Benchmarks](../README.md#benchmarks-job) section has the methodology.

### When Robust engages

- **≥ 2 joins required.** Single-join queries get no benefit from cross-graph propagation (JFP already handles them). `RobustOptimizerContextState::Optimize` checks `edges.size() <= 1` and returns the plan unchanged ([`src/optimizer/robust_optimizer.cpp:1603`](../src/optimizer/robust_optimizer.cpp)).
- **Equality joins only.** Non-equality predicates aren't tracked. Range joins flow through but produce no filters.
- **Acyclic graphs.** The current focus is on acyclic join graphs, where Robust's behaviour is well understood. Behaviour on cyclic join graphs is not characterised yet — they may work, may degrade, or may crash; characterising and handling them properly is on the near-term roadmap.

---

## 2. The rewrite at a glance

![Baseline vs. Robust plan](figures/arch/d1-baseline-vs-robust.png)

The diagram above shows a 4-way join (`orders ⋈ customers ⋈ products ⋈ categories`) under stock DuckDB on the left and under Robust on the right. Both plans return identical rows.

The Robust plan inserts two kinds of new operator:

- **`CREATE_FILTER`** sits above any base scan that contributes to a join. It's a parallel sink: it consumes the entire build-side chunk stream, builds the filters in thread-local state, merges them globally on combine, and finalizes them once all input is seen.
- **`PROBE_FILTER`** sits above the *probe-side* scan of a join. It applies the filters produced by its paired `CREATE_FILTER` to incoming rows so that only rows with a chance of matching downstream make it into the hash join. The exact mechanism — whether the filter is evaluated per-row inside the operator or pushed all the way into the underlying scan as a dynamic filter — depends on which pass produced it, and is covered in §6.

The salmon-shaded boxes in every diagram throughout this doc represent extension operators we add; outline-only boxes are stock DuckDB operators we don't modify. Where the `PROBE_FILTER` border is *dashed*, the filter has been pushed into the underlying scan as a dynamic filter rather than evaluated by the operator itself (see §6.3).

Mental model in one paragraph: the rest of this doc explains (a) how we decide *which* `CREATE_FILTER`s and `PROBE_FILTER`s to insert and *where* — that's the DAG and the two passes; (b) what runs *inside* each operator — that's §6; and (c) which *filter types* it builds and pushes — that's §7. The rest is edge cases.

---

## 3. The join graph (DAG)

### 3.1 Equivalence classes

A query's equality joins partition all column bindings into **equivalence classes**: sets of `(table_index, column_index)` pairs that are transitively equal via join predicates. For example, in

```sql
FROM A JOIN B ON A.x = B.x
       JOIN C ON B.x = C.y
```

the bindings `{A.x, B.x, C.y}` form a single equivalence class — any filter derived from any of them is semantically valid on the other two.

Robust tracks these via a union-find structure built incrementally as the plan is traversed in `ExtractOperators` and `ExtractOperatorsRecursive` ([`src/optimizer/robust_optimizer.cpp`](../src/optimizer/robust_optimizer.cpp), around line 50 onward). DELIM_JOINs are treated identically to comparison joins for edge extraction, which lets decorrelated subqueries (TPC-H Q17, Q21) participate; MARK_JOINs have their probe child recursed into but their build child (a column-data scan of IN-list values) is skipped, since it's not a real base table.

### 3.2 Heuristic A — `largest_root`

The `largest_root` heuristic is the formulation from the **original predicate transfer paper** (Yang et al., CIDR 2024 — linked in §1). It builds a **minimum spanning tree** (Prim's algorithm) over the join edges, growing from the table with highest base cardinality. The MST is then rooted at that same table via BFS (`BuildRootedTree`).

![DAG under largest_root](figures/arch/d2a-dag-largest-root.png)

It tends to produce well-balanced trees where the largest table is the anchor and small dim tables become leaves. Forward and backward passes are generated by the tree-based `GenerateStageModifications`.

### 3.3 Heuristic B — `join_order` (default)

`join_order` is a **new heuristic introduced by this extension** — it does not appear in the original paper — and is what we ship as the default. Instead of building an MST, it mirrors *DuckDB's own join execution order* by running a build-first DFS over the logical plan: at each join, the right (build) child is visited before the left (probe) child. The resulting **DFS index** of each base-table node encodes execution order — a lower index means earlier (build-side) execution.

For each equality join condition, both column bindings are resolved through `rename_col_bindings` (a map that tracks renames across projections and aggregates), union-find merges them, and a directed edge is added **from higher DFS index to lower DFS index** — i.e. from the later-executed table to its build-side feeder.

![DAG under join_order](figures/arch/d2b-dag-join-order.png)

**Why this works:** DuckDB has already done expensive cardinality estimation and plan enumeration before our optimizer runs. Its join order already reflects "build the smaller side, probe with the larger" decisions. By following the same order, our filter cascades fire in the same sequence as the hash-join pipelines, which maximises early pruning on linear join chains.

The implementation is in `BuildPhysicalPlanDAG` and `GenerateStageModificationsFromDAG` ([`src/optimizer/robust_optimizer.cpp:1619–1648`](../src/optimizer/robust_optimizer.cpp)).

### 3.4 Iterative root flipping

A `join_order` DAG can have **multiple roots** — nodes with no parents. This happens when small dimensions are at the "top" of the join order. The problem: if a fact table sits one level down with a small-dim root above it, the fact table is on the probe side of its only edge but has no parent driving a filter into it. The largest table — the one we most want to filter — ends up doing the full scan.

The fix is `FlipRootsToLeaves` ([`src/optimizer/robust_optimizer.cpp:718–813`](../src/optimizer/robust_optimizer.cpp)). It identifies all roots, keeps the one with highest cardinality as the **anchor root**, and iteratively reverses edges from every other root to its children until only the anchor has no parents.

The *iterative* part matters: a single-pass flip can orphan intermediate nodes (flipping `name → aka_name` makes `aka_name` parentless, creating a new non-anchor root). The while-loop at [`robust_optimizer.cpp:741`](../src/optimizer/robust_optimizer.cpp) keeps flipping until the graph is stable. See [feature 5](../wip_docs/features/5-scan-table-filters-bypass.md) for the bug story.

Gated by `robust_flip_roots` (default `true`). Set it to `false` to compare against pre-flip behaviour.

### 3.5 What the optimizer accepts

The DAG extractor walks past several DuckDB operator types it considers "transparent":

| Operator | Treatment |
|---|---|
| `LOGICAL_COMPARISON_JOIN` | Edge source — extracts equality conditions |
| `LOGICAL_DELIM_JOIN` | Same as above (decorrelated subqueries) |
| `LOGICAL_FILTER`, `LOGICAL_PROJECTION` | Walked through; renames tracked |
| `LOGICAL_MARK_JOIN` | Probe child recursed, build child skipped |
| `LOGICAL_AGGREGATE` | Currently a barrier — [feature 11](../wip_docs/features/11-bf-above-aggregate-and-compress.md) plans to lift CREATE_FILTERs above |
| `__internal_compress_integral_*` projections | Currently a barrier — same feature 11 plan |

---

## 4. Forward pass

![Forward pass](figures/arch/d3-forward-pass.png)

The forward pass walks the DAG **bottom-up** — from leaves to root. For each child node at depth L, for each edge to a parent: emit a `CREATE_FILTER` on the child (building filters from the child's join column) and a `PROBE_FILTER` on the parent (using those filters to filter the parent's join column).

The output of the forward pass is a stream of `FilterOperation` records, each marked with `is_forward_pass = true` and a `sequence_number` preserving generation order (needed for stack insertion later).

Where the filters are *built* — meaning what rows feed into the `CREATE_FILTER` sink — matters enormously. If a `CREATE_FILTER` sits below a `FILTER` operator, it sees the unfiltered rows and produces a too-broad filter. Two lift passes ensure the `CREATE_FILTER` sees the *post-filter* row set:

- **`LiftCreateFilterAboveMarkJoin`** ([`robust_optimizer.cpp:1528–1559`](../src/optimizer/robust_optimizer.cpp)) detects `LOGICAL_FILTER(child = LOGICAL_MARK_JOIN)` patterns — DuckDB's representation of `IN` predicates — and moves the `CREATE_FILTER` stack from below the `MARK_JOIN`'s probe child up to just above the `FILTER`. Without this, the `cast_info` filter on JOB 18c builds a bloom filter from 8.2M rows instead of the 1.2M that survive the `IN` predicate ([feature 6](../wip_docs/features/6-lift-create-bf-above-mark-join.md)).
- **`LiftCreateFilterAboveFilter`** ([`robust_optimizer.cpp:1561–1586`](../src/optimizer/robust_optimizer.cpp)) handles the plain-`FILTER` analogue — `CREATE_FILTER` lifts above any inline filter that sits between it and the underlying scan.

After lifting, `LinkProbeFilterToCreateFilter` walks the plan twice to wire each `LogicalProbeFilter` to its matching `LogicalCreateFilter` via the `related_create_filter` and `related_probe_filter` vectors. This is what makes the backward pass's per-row `LookupSel` (see §6.2) able to find its filter.

Finally, `SetupDynamicFilterPushdown` runs for forward-pass `CREATE_FILTER`s **only**: it locates each related probe's `LogicalGet`, attaches a `DynamicTableFilterSet`, and registers the `LogicalCreateFilter` to populate that set during `Finalize`. The forward-pass `PROBE_FILTER`s are marked `is_passthrough = true` — the real filtering happens at the scan now.

---

## 5. Backward pass

![Backward pass](figures/arch/d4-backward-pass.png)

The backward pass walks the DAG **top-down** — from root to leaves — and propagates filters discovered late in the plan back into scans earlier in the plan. Where the forward pass builds filters bottom-up from each leaf, the backward pass takes filters that already exist higher in the tree and reuses them downstream, shrinking intermediate results before they have a chance to explode.

This is the main thing that distinguishes Robust from DuckDB's native JFP (§1) and is where the bulk of the JOB workload's wins come from. On `title`-anchored queries — where many tables join on `movie_id` — one filter derived from a highly selective `title` predicate ends up pruning a dozen other probe-side scans.

### How it works under the hood

For each child node, parents are sorted by **cardinality ascending** (smallest first — the most selective filter applies earliest). For each parent edge, the pass consults the union-find structure for the **equivalence class** of the edge's column (§3.1) — the set of `(table, column)` pairs the join graph has proven transitively equal:

- If that class **already has a `CREATE_FILTER` source** from an ancestor traversal, only emit a `USE` operation on the child, pointing at the ancestor's existing filter. No new bloom filter is built; the existing filter is reused across the class.
- Otherwise, emit both a `CREATE` on the parent (because that's where the filter is built) and a `USE` on the child, recording the parent as the new source for that equivalence class.

Unlike the forward pass, the backward pass does **not** currently push its filters into scans as dynamic filters. Its filters apply per-row inside `PROBE_FILTER::Execute` via the bloom filter's `LookupSel` — see §6.2 for the operator side and §6.3 for the dynamic-pushdown mechanism the forward pass uses.

---

## 6. Operator internals

### 6.1 `CREATE_FILTER` as a parallel sink

![CREATE_FILTER sink lifecycle](figures/arch/d5-create-filter-lifecycle.png)

`PhysicalCreateFilter` implements DuckDB's parallel sink interface (`IsSink() = true`, `ParallelSink() = true`):

| Phase | What it does |
|---|---|
| `GetGlobalSinkState` | Allocates one `PTBloomFilter` per build column at the operator's `estimated_cardinality`. Resolves the shared `probe_empty_flag` from `ProbeEmptyRegistry` if the operator is in the forward pass. |
| `Sink(chunk)` | Per-thread: checks `probe_empty_flag` and short-circuits if set; appends the chunk to a thread-local `ColumnDataCollection`; inserts hashes into the BF; (forward pass only) updates thread-local min/max and tracks distinct values up to `threshold + 1`. |
| `Combine` | Under mutex: merges thread-local `ColumnDataCollection` / min-max / distinct sets into the global state. |
| `Finalize` | Concatenates per-thread collections into a single `total_data` collection. Runs the **BF resize check**: if `actual_rows * 8 > allocated_bits` (i.e. we're under 8 bits/key, ≥ 2.3% FPR), tear down the BF and call `PTBloomFilter::ReinitializeAndRehash` to rebuild at the correct size. Sets `probe_empty_flag` to `true` if `actual_rows == 0`. Calls `PushDynamicFilters`. |
| `PushDynamicFilters` (forward only) | For each `pushdown_target`, picks the right filter type (see §7) and installs it on the target `DynamicTableFilterSet`. |

Two of the most material decisions inside this lifecycle:

**BF resize at Finalize** ([`physical_create_filter.cpp:493–513`](../src/operators/physical_create_filter.cpp)). The BF is sized at plan time using optimizer cardinality estimates, which can be 5–10× wrong. If we under-allocate, the resulting bloom filter is so dense (1.68 bits/key in pathological cases) that its FPR approaches 65% — effectively a no-op. The resize at Finalize uses the actual row count, targeting ≥ 8 bits/key (≤ 2.3% FPR). On the JOB suite this single change moved geomean speedup from **1.132× to 1.193×** ([feature 7](../wip_docs/features/7-accurate-bf-sizing.md)).

**Empty-probe short-circuit** ([feature 8](../wip_docs/features/8-short-circuit-empty-probe.md), [`physical_create_filter.cpp:199–203, 236–238, 524–526`](../src/operators/physical_create_filter.cpp)). Multiple `CREATE_FILTER`s often target the same probe-side table. If one of them finalizes with zero rows (the filter on the build side is so selective nothing survives), it sets a shared `atomic<bool>` on the `ProbeEmptyRegistry` for that probe. Sibling `CREATE_FILTER`s targeting the same probe check that flag at the top of every `Sink` call and immediately return `SinkResultType::FINISHED`, draining their pipelines without doing further build work. On JOB 32a this saved 3.6ms on a 186K-row scan that would have been entirely pointless.

### 6.2 `PROBE_FILTER` — two execution modes

`PhysicalProbeFilter` extends `CachingPhysicalOperator`. It has two distinct execution behaviours depending on which pass created it:

**Forward-pass (`is_passthrough = true`).** The filtering work has been hoisted into the underlying scan's `DynamicTableFilterSet`. The operator itself does nothing useful at runtime — `ExecuteInternal` just calls `chunk.Reference(input)`. The operator still exists in the plan because it makes the dataflow legible in `EXPLAIN` output and because it's where the `LogicalProbeFilter ↔ LogicalCreateFilter` link is anchored.

**Backward-pass.** The operator actually filters chunks per-row. On first call it lazily initialises a vector of `PTBloomFilter` references from `related_create_filter_vec` (because the backward pass can have multiple build sides feeding one probe). Each chunk passes through `PTBloomFilter::LookupSel`, which produces a `SelectionVector` of surviving rows; the operator then slices the chunk and emits.

The dashed border on `PROBE_FILTER` boxes in every diagram throughout this doc marks the passthrough variant.

### 6.3 Dynamic filter pushdown into scans

![Dynamic filter pushdown](figures/arch/d6-dynamic-pushdown.png)

For forward-pass `CREATE_FILTER`s, the optimizer installs the filter on `LogicalGet.dynamic_filters` during planning (`SetupDynamicFilterPushdown`). When DuckDB later constructs the physical `PhysicalTableScan`, it picks up `dynamic_filters` from the `LogicalGet` and consults them at scan time via `DynamicTableFilterSet::GetCombinedFilter`.

The result: the scan does both **zonemap-based row-group pruning** (entire row groups skipped without reading) *and* **per-row filtering** as appropriate to the filter type. The `PROBE_FILTER` operator above doesn't have to do anything because the rows that would have been filtered never make it out of the scan.

This pushdown is **forward-pass only**. Backward-pass filters apply at the `PROBE_FILTER` operator instead.

---

## 7. Filter types and the picker

![Filter-type picker](figures/arch/d7-filter-picker.png)

Robust ships four filter types today; the picker logic lives in `PhysicalCreateFilter::PushDynamicFilters` ([`physical_create_filter.cpp:364`](../src/operators/physical_create_filter.cpp)).

| Filter | Implementation | Push form | When chosen |
|---|---|---|---|
| **Always-false** | n/a | `ConstantFilter(GREATERTHAN, MaximumValue(type))` | Build side has 0 rows |
| **Equality constant** | n/a | `ConstantFilter(EQUAL, v)` | Exactly 1 distinct build value |
| **IN-list** | `value_set_t` (capped, dynamic-or-filter) | `OptionalFilter(InFilter(values))` | Distinct count ≤ `robust_dynamic_or_filter_threshold` (default 50) |
| **Bloom + min/max** | `PTBloomFilter` + `ColumnMinMax` | `SelectivityOptionalFilter(BFTableFilter)` plus two `ConstantFilter`s for min and max | Otherwise — the default path |

The **IN-list path** ([feature 9](../wip_docs/features/9-in-filter-for-small-build-side.md)) is the most consequential addition beyond the original paper. When a build side has a small number of distinct values, pushing an exact `IN`-list enables tighter zonemap pruning than a bloom filter does — for PK-clustered columns this can mean landing in a single row group versus the BF's "could be anywhere in the cardinality range". On a full JOB sweep this change improved 37 queries with zero regressions and reduced total rows scanned by **−84.7M** in aggregate.

**Filter-type override.** The `robust_filter_type` setting (default `"all"`) can force the picker to a subset: `"bf_only"`, `"minmax_only"`. Useful for ablation studies.

### Bloom filter internals

We wrap DuckDB's native `duckdb::BloomFilter` in `PTBloomFilter` ([`src/include/bloom_filter.hpp`](../src/include/bloom_filter.hpp), [`src/bloom_filter.cpp`](../src/bloom_filter.cpp)). The wrapper adds:

- `DataChunk`-level `Insert` / `LookupSel` operations
- Resize-and-rehash for the cardinality-mismatch case (§6.1)
- An `IsEmpty` check (`!bf_.IsInitialized()`)

Insertion uses `BloomFilter::InsertHashes`, which is atomic via `fetch_or` (safe for parallel sink threads). Lookup returns a `SelectionVector` directly via `LookupSel`, avoiding an intermediate `uint32_t` array allocation.

We don't implement the bloom filter ourselves — that's DuckDB's split-block design with 12 bits/key and 4 bits set per insert. Replacing the earlier hand-rolled implementation with the native one was a significant cleanup; see `wip_docs/bloom_filter_comparison.md` for the migration notes.

---

## 8. Edge cases and tricks

A non-exhaustive list of things the code handles explicitly because they bit us at some point:

- **MARK_JOIN probe-only DFS** ([`robust_optimizer.cpp:549–551`](../src/optimizer/robust_optimizer.cpp)). MARK_JOIN's build child is a column-data scan of `IN`-list values, not a real table; recursing into it would corrupt the DAG. Only the probe child is walked.
- **DELIM_JOIN acceptance** ([`robust_optimizer.cpp:50–51`](../src/optimizer/robust_optimizer.cpp)). Decorrelated subqueries (TPC-H Q17, Q21) use `LOGICAL_DELIM_JOIN`; treating it identically to `LOGICAL_COMPARISON_JOIN` for edge extraction lets these queries participate in transfer.
- **Consecutive `CREATE` merging** ([`BuildStackedBFOperators`, robust_optimizer.cpp:1183–1214](../src/optimizer/robust_optimizer.cpp)). Multiple `FilterOperation`s targeting the same `build_table_idx` get merged into a single `LogicalCreateFilter` with multiple build/probe column pairs. Fewer pipeline breaks, fewer redundant materializations.
- **Lift above MARK_JOIN / FILTER** (§4). Without these, `CREATE_FILTER`s build their filters from the wrong row set.
- **Iterative root flipping** (§3.4). Single-pass flipping orphans intermediate nodes; the while-loop iterates until stable.
- **Single-join early exit** ([`robust_optimizer.cpp:1603`](../src/optimizer/robust_optimizer.cpp)). `edges.size() <= 1` returns the plan unchanged — predicate transfer needs at least two joins to have anything to transfer.
- **Cyclic join graph OOB.** Cyclic graphs (TPC-H Q5, Q9, Q20) produce equivalence classes that `LogicalCreateFilter::CreatePlan` can't fully resolve, causing an out-of-bounds in `bound_column_indices` at sink time. Currently handled by skip-listing those queries in the test harness; a proper detect-and-bail guard is [feature 10](../wip_docs/features/10-tpch-workload-integration.md) work.

---

---

**Last updated:** 2026-05. Audit-of-record against current `main` (commits `08bdd01` and earlier).
