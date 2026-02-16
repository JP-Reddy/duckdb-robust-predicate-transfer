//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "blocked_bloom_filter.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <cstdint>

namespace duckdb {

class BloomFilter {
public:
	BloomFilter() = default;
	void Initialize(ClientContext &context_p, uint32_t est_num_rows);

	ClientContext *context = nullptr;
	BufferManager *buffer_manager = nullptr;

	bool finalized_ = false;

public:
	int Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied) const;
	void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built);
	void Fold();

	bool IsEmpty() const {
		return bbf_.IsEmpty();
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

// builder for constructing bloom filter from raw data
// it is used for parallel finalize with atomic bloom filter operations
class BloomFilterBuilder {
public:
	BloomFilterBuilder() = default;

	// initialize builder with target bf and columns to hash
	void Begin(shared_ptr<BloomFilter> bf, const vector<idx_t> &bound_cols);

	void PushNextBatch(DataChunk &chunk) const;

	vector<idx_t> BuiltCols() const;
private:
	shared_ptr<BloomFilter> bloom_filter;
	vector<idx_t> bound_cols;
};

} // namespace duckdb
