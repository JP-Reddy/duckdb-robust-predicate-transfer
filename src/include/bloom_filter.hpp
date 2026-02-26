//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/predicate_transfer/bloom_filter/bloom_filter.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/types/data_chunk.hpp"

#include <cstdint>

namespace duckdb {

// wrapper around DuckDB's native BloomFilter with DataChunk-level operations
class PTBloomFilter {
public:
	PTBloomFilter() = default;
	void Initialize(ClientContext &context_p, uint32_t est_num_rows);

	ClientContext *context = nullptr;
	BufferManager *buffer_manager = nullptr;

	bool finalized_ = false;

public:
	int Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied,
	           uint8_t *bit_vector_buf = nullptr) const;
	idx_t LookupSel(DataChunk &chunk, SelectionVector &sel, const vector<idx_t> &bound_cols_applied,
	                uint8_t *bit_vector_buf) const;
	void Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built);

	bool IsEmpty() const {
		return !bf_.IsInitialized();
	}

	BloomFilter &GetNativeFilter() {
		return bf_;
	}

private:
	BloomFilter bf_;
};

} // namespace duckdb
