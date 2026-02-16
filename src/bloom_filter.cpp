#include "bloom_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"

namespace duckdb {

BloomFilterMasks BlockedBloomFilter::masks_;

namespace {
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
	bbf_.CreateEmpty(static_cast<int64_t>(est_num_rows));
}

int BloomFilter::Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied) const {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	auto hash_data = reinterpret_cast<const uint64_t *>(hashes.GetData());

	// BlockedBloomFilter returns results as a bit vector
	vector<uint8_t> bit_vector((count + 7) / 8, 0);
	bbf_.Find(count, hash_data, bit_vector.data());

	// convert bit vector to uint32_t results (1 or 0 per row)
	for (int i = 0; i < count; i++) {
		results[i] = (bit_vector[i / 8] >> (i % 8)) & 1;
	}
	return count;
}

void BloomFilter::Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_built);
	auto hash_data = reinterpret_cast<const uint64_t *>(hashes.GetData());
	bbf_.InsertAtomic(count, hash_data);
}

void BloomFilter::Fold() {
	bbf_.Fold();
}

// initialize builder with target bf and columns to hash
void BloomFilterBuilder::Begin(shared_ptr<BloomFilter> bf, const vector<idx_t> &bound_cols) {
	bloom_filter = bf;
	this->bound_cols = bound_cols;
}

void BloomFilterBuilder::PushNextBatch(DataChunk &chunk) const {
	bloom_filter->Insert(chunk, bound_cols);
}

vector<idx_t> BloomFilterBuilder::BuiltCols() const {
	return bound_cols;
}

} // namespace duckdb
