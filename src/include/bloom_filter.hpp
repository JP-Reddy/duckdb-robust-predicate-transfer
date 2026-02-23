//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "blocked_bloom_filter.hpp"
#include "partition_util.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include <cstdint>

namespace duckdb {

// Build strategy for bloom filter construction
enum class BloomFilterBuildStrategy {
	SINGLE_THREADED = 0,
	PARALLEL = 1
};

class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, uint32_t est_num_rows);

	ClientContext *context = nullptr;
	BufferManager *buffer_manager = nullptr;

	bool finalized_ = false;

public:
	int Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied,
	           uint8_t *bit_vector_buf = nullptr) const;
	// direct bit vector → selection vector (no intermediate uint32_t array)
	idx_t LookupSel(DataChunk &chunk, SelectionVector &sel, const vector<idx_t> &bound_cols_applied,
	                uint8_t *bit_vector_buf) const;
	void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built);
	void InsertHashes(int64_t num_rows, const uint64_t *hashes);
	void Fold();

	bool IsEmpty() const {
		return bbf_.IsEmpty();
	}

	// access to underlying blocked bloom filter for parallel builders
	BlockedBloomFilter &GetBlockedBloomFilter() {
		return bbf_;
	}

private:
	BlockedBloomFilter bbf_;
};

class BloomFilterUsage {
public:
	BloomFilterUsage(shared_ptr<BloomFilter> bloom_filter, const vector<idx_t> &applied, const vector<idx_t> &built)
	    : bloom_filter(std::move(bloom_filter)), bound_cols_applied(applied), bound_cols_built(built) {
	}

	bool IsValid() const {
		return bloom_filter->finalized_;
	}

public:
	int Lookup(DataChunk &chunk, vector<uint32_t> &results) const {
		return bloom_filter->Lookup(chunk, results, bound_cols_applied);
	}
	void Insert(DataChunk &chunk) const {
		return bloom_filter->Insert(chunk, bound_cols_applied);
	}

private:
	shared_ptr<BloomFilter> bloom_filter;
	vector<idx_t> bound_cols_applied;
	vector<idx_t> bound_cols_built;
};

// Abstract builder interface for constructing bloom filters
class BloomFilterBuilderBase {
public:
	virtual ~BloomFilterBuilderBase() = default;

	// initialize builder with target bf and columns to hash
	virtual void Begin(size_t num_threads, shared_ptr<BloomFilter> bf, const vector<idx_t> &bound_cols) = 0;

	// push a batch of data, called from potentially multiple threads
	// thread_id identifies the calling thread (0 to num_threads-1)
	virtual void PushNextBatch(size_t thread_id, DataChunk &chunk) = 0;

	// clean up resources after building is complete
	virtual void CleanUp() {}

	virtual vector<idx_t> BuiltCols() const = 0;

	// factory method to create builder based on strategy
	static unique_ptr<BloomFilterBuilderBase> Make(BloomFilterBuildStrategy strategy);
};

// Single-threaded builder - simple, no synchronization needed
class BloomFilterBuilder_SingleThreaded : public BloomFilterBuilderBase {
public:
	BloomFilterBuilder_SingleThreaded() = default;

	void Begin(size_t num_threads, shared_ptr<BloomFilter> bf, const vector<idx_t> &bound_cols) override;
	void PushNextBatch(size_t thread_id, DataChunk &chunk) override;
	vector<idx_t> BuiltCols() const override;

private:
	shared_ptr<BloomFilter> bloom_filter_;
	vector<idx_t> bound_cols_;
};

// Parallel builder - uses partition-based locking to minimize contention
class BloomFilterBuilder_Parallel : public BloomFilterBuilderBase {
public:
	BloomFilterBuilder_Parallel() = default;

	void Begin(size_t num_threads, shared_ptr<BloomFilter> bf, const vector<idx_t> &bound_cols) override;
	void PushNextBatch(size_t thread_id, DataChunk &chunk) override;
	void CleanUp() override;
	vector<idx_t> BuiltCols() const override;

private:
	void PushNextBatchImpl(size_t thread_id, int64_t num_rows, const uint64_t *hashes);

	shared_ptr<BloomFilter> bloom_filter_;
	vector<idx_t> bound_cols_;

	int log_num_prtns_;
	PartitionLocks prtn_locks_;

	// per-thread state for partitioning
	struct ThreadLocalState {
		vector<uint64_t> partitioned_hashes;
		vector<uint16_t> partition_ranges;
		vector<int> unprocessed_partition_ids;
	};
	vector<ThreadLocalState> thread_local_states_;
};

// Legacy builder wrapper for backward compatibility
// Defaults to auto-selecting strategy based on thread count
class BloomFilterBuilder {
public:
	BloomFilterBuilder() = default;

	// initialize builder with target bf and columns to hash
	void Begin(shared_ptr<BloomFilter> bf, const vector<idx_t> &bound_cols, size_t num_threads = 1);

	// push a batch of data (single-threaded interface for compatibility)
	void PushNextBatch(DataChunk &chunk) const;

	// push a batch with explicit thread ID (parallel interface)
	void PushNextBatch(size_t thread_id, DataChunk &chunk) const;

	void CleanUp();

	vector<idx_t> BuiltCols() const;

private:
	mutable unique_ptr<BloomFilterBuilderBase> impl_;
	size_t num_threads_ = 1;
};

} // namespace duckdb
