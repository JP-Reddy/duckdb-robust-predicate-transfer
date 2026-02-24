#include "bloom_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

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

void PTBloomFilter::Initialize(ClientContext &context_p, uint32_t est_num_rows) {
	context = &context_p;
	buffer_manager = &BufferManager::GetBufferManager(*context);
	bf_.Initialize(context_p, static_cast<idx_t>(est_num_rows));
}

int PTBloomFilter::Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied,
                          uint8_t *bit_vector_buf) const {
	int count = static_cast<int>(chunk.size());
	if (count == 0) {
		return 0;
	}
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	auto hash_data = FlatVector::GetData<hash_t>(hashes);

	for (int i = 0; i < count; i++) {
		results[i] = bf_.LookupOne(hash_data[i]) ? 1 : 0;
	}
	return count;
}

idx_t PTBloomFilter::LookupSel(DataChunk &chunk, SelectionVector &sel, const vector<idx_t> &bound_cols_applied,
                                uint8_t *bit_vector_buf) const {
	idx_t count = chunk.size();
	if (count == 0) {
		return 0;
	}
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	return bf_.LookupHashes(hashes, sel, count);
}

void PTBloomFilter::Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	idx_t count = chunk.size();
	if (count == 0) {
		return;
	}
	Vector hashes = HashColumns(chunk, bound_cols_built);
	bf_.InsertHashes(hashes, count);
}

} // namespace duckdb
