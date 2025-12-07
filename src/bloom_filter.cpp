#include "bloom_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

#include <random>
#include <cmath>
#include <iostream>

namespace duckdb {
namespace {
static uint32_t CeilPowerOfTwo(uint32_t n) {
	if (n <= 1) {
		return 1;
	}
	n--;
	n |= (n >> 1);
	n |= (n >> 2);
	n |= (n >> 4);
	n |= (n >> 8);
	n |= (n >> 16);
	return n + 1;
}

static Vector HashColumns(DataChunk &chunk, const vector<idx_t> &cols) {
	auto count = chunk.size();
	Vector hashes(LogicalType::HASH);
	VectorOperations::Hash(chunk.data[cols[0]], hashes, count);
	for (size_t j = 1; j < cols.size(); j++) {
		VectorOperations::CombineHash(hashes, chunk.data[cols[j]], count);
	}

	if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		hashes.Flatten(count);
	}

	return hashes;
}
} // namespace

void BloomFilter::Initialize(ClientContext &context_p, uint32_t est_num_rows) {
	context = &context_p;
	buffer_manager = &BufferManager::GetBufferManager(*context);

	uint32_t min_bits = std::max<uint32_t>(MIN_NUM_BITS, est_num_rows * MIN_NUM_BITS_PER_KEY);
	num_sectors = std::min(CeilPowerOfTwo(min_bits) >> LOG_SECTOR_SIZE, MAX_NUM_SECTORS);
	num_sectors_log = static_cast<uint32_t>(std::log2(num_sectors));

	buf_ = buffer_manager->GetBufferAllocator().Allocate(64 + num_sectors * sizeof(uint32_t));
	// make sure blocks is a 64-byte aligned pointer, i.e., cache-line aligned
	blocks = reinterpret_cast<std::atomic<uint32_t> *>((64ULL + reinterpret_cast<uint64_t>(buf_.get())) & ~63ULL);
	// std::fill_n(blocks, num_sectors, 0);
	for (uint32_t i = 0; i < num_sectors; i++) {
		blocks[i].store(0, std::memory_order_relaxed);
	}
}

int BloomFilter::Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied) const {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	BloomFilterLookup(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks, results.data());
	return count;
}

void BloomFilter::Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_built);
	BloomFilterInsert(count, reinterpret_cast<uint64_t *>(hashes.GetData()), blocks);
}

// initialize builder with target bf and columns to hash
void BloomFilterBuilder::Begin(shared_ptr<BloomFilter> bf, const vector<idx_t> &bound_cols) {
	bloom_filter = bf;
	this->bound_cols = bound_cols;
}

void BloomFilterBuilder::PushNextBatch(int64_t num_rows, const uint64_t *hashes) const {
	// create temp chunk for hashing
	DataChunk temp_chunk;
	temp_chunk.Initialize(Allocator::DefaultAllocator(),{LogicalType::HASH});
	temp_chunk.SetCardinality(num_rows);

	// copy hashes to chunk
	auto hash_data = FlatVector::GetData<uint64_t>(temp_chunk.data[0]);
	memcpy(hash_data, hashes, num_rows * sizeof(hash_t));

	vector<idx_t> hash_col = {0};
	bloom_filter->Insert(temp_chunk, hash_col);
}

vector<idx_t> BloomFilterBuilder::BuiltCols() const {
	return bound_cols;
}

} // namespace duckdb