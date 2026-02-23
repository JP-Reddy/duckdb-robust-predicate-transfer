#include "bloom_filter.hpp"

#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

namespace duckdb {

BloomFilterMasks BlockedBloomFilter::masks_;

// threshold for switching to parallel build (in rows)
// empirically tuned: 10k produces best results on JOB benchmark
// partition setup overhead dominates for smaller builds
static constexpr idx_t PARALLEL_BUILD_ROW_THRESHOLD = 10000;

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

int BloomFilter::Lookup(DataChunk &chunk, vector<uint32_t> &results, const vector<idx_t> &bound_cols_applied,
                        uint8_t *bit_vector_buf) const {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	auto hash_data = reinterpret_cast<const uint64_t *>(hashes.GetData());

	int64_t bv_bytes = (count + 7) / 8;
	vector<uint8_t> local_bv;
	if (!bit_vector_buf) {
		local_bv.resize(bv_bytes, 0);
		bit_vector_buf = local_bv.data();
	} else {
		memset(bit_vector_buf, 0, bv_bytes);
	}

	bbf_.Find(count, hash_data, bit_vector_buf);

	// convert bit vector to uint32_t results (1 or 0 per row)
	for (int i = 0; i < count; i++) {
		results[i] = (bit_vector_buf[i / 8] >> (i % 8)) & 1;
	}
	return count;
}

idx_t BloomFilter::LookupSel(DataChunk &chunk, SelectionVector &sel, const vector<idx_t> &bound_cols_applied,
                             uint8_t *bit_vector_buf) const {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_applied);
	auto hash_data = reinterpret_cast<const uint64_t *>(hashes.GetData());

	int64_t bv_bytes = (count + 7) / 8;
	memset(bit_vector_buf, 0, bv_bytes);
	bbf_.Find(count, hash_data, bit_vector_buf);

	// scan set bits directly into selection vector
	idx_t result_count = 0;
	auto *words = reinterpret_cast<const uint64_t *>(bit_vector_buf);
	int64_t num_full_words = count / 64;

	for (int64_t w = 0; w < num_full_words; w++) {
		uint64_t bits = words[w];
		idx_t base = static_cast<idx_t>(w * 64);
		while (bits != 0) {
			sel.set_index(result_count++, base + __builtin_ctzll(bits));
			bits &= bits - 1;
		}
	}

	// remaining rows
	int remaining = count - static_cast<int>(num_full_words * 64);
	if (remaining > 0) {
		uint64_t bits = 0;
		memcpy(&bits, bit_vector_buf + num_full_words * 8, (remaining + 7) / 8);
		bits &= (1ULL << remaining) - 1;
		idx_t base = static_cast<idx_t>(num_full_words * 64);
		while (bits != 0) {
			sel.set_index(result_count++, base + __builtin_ctzll(bits));
			bits &= bits - 1;
		}
	}

	return result_count;
}

void BloomFilter::Insert(DataChunk &chunk, const vector<idx_t> &bound_cols_built) {
	int count = static_cast<int>(chunk.size());
	Vector hashes = HashColumns(chunk, bound_cols_built);
	auto hash_data = reinterpret_cast<const uint64_t *>(hashes.GetData());
	bbf_.Insert(count, hash_data);
}

void BloomFilter::InsertHashes(int64_t num_rows, const uint64_t *hashes) {
	// non-atomic insert for use under partition lock
	bbf_.Insert(num_rows, hashes);
}

void BloomFilter::Fold() {
	bbf_.Fold();
}

//===--------------------------------------------------------------------===//
// BloomFilterBuilderBase Factory
//===--------------------------------------------------------------------===//

unique_ptr<BloomFilterBuilderBase> BloomFilterBuilderBase::Make(BloomFilterBuildStrategy strategy) {
	switch (strategy) {
	case BloomFilterBuildStrategy::SINGLE_THREADED:
		return make_uniq<BloomFilterBuilder_SingleThreaded>();
	case BloomFilterBuildStrategy::PARALLEL:
		return make_uniq<BloomFilterBuilder_Parallel>();
	default:
		return make_uniq<BloomFilterBuilder_SingleThreaded>();
	}
}

//===--------------------------------------------------------------------===//
// BloomFilterBuilder_SingleThreaded
//===--------------------------------------------------------------------===//

void BloomFilterBuilder_SingleThreaded::Begin(size_t num_threads, shared_ptr<BloomFilter> bf,
                                              const vector<idx_t> &bound_cols) {
	bloom_filter_ = std::move(bf);
	bound_cols_ = bound_cols;
}

void BloomFilterBuilder_SingleThreaded::PushNextBatch(size_t thread_id, DataChunk &chunk) {
	bloom_filter_->Insert(chunk, bound_cols_);
}

vector<idx_t> BloomFilterBuilder_SingleThreaded::BuiltCols() const {
	return bound_cols_;
}

//===--------------------------------------------------------------------===//
// BloomFilterBuilder_Parallel
//===--------------------------------------------------------------------===//

void BloomFilterBuilder_Parallel::Begin(size_t num_threads, shared_ptr<BloomFilter> bf,
                                        const vector<idx_t> &bound_cols) {
	bloom_filter_ = std::move(bf);
	bound_cols_ = bound_cols;

	// determine number of partitions based on log_num_blocks
	// we want to keep groups of blocks together to ensure cache locality
	constexpr int kLogBlocksKeptTogether = 7; // 128 blocks per partition
	constexpr int kMaxLogNumPrtns = 8;        // max 256 partitions

	int log_num_blocks = bloom_filter_->GetBlockedBloomFilter().LogNumBlocks();
	int log_num_prtns_max = std::max(0, log_num_blocks - kLogBlocksKeptTogether);
	log_num_prtns_ = std::min(kMaxLogNumPrtns, std::min(log_num_prtns_max, Log2Ceil(static_cast<int64_t>(num_threads))));

	int num_prtns = 1 << log_num_prtns_;

	// initialize partition locks
	prtn_locks_.Init(num_threads, num_prtns);

	// initialize per-thread state
	thread_local_states_.resize(num_threads);
}

void BloomFilterBuilder_Parallel::PushNextBatch(size_t thread_id, DataChunk &chunk) {
	int count = static_cast<int>(chunk.size());
	if (count == 0) {
		return;
	}

	// hash the columns
	Vector hashes = HashColumns(chunk, bound_cols_);
	auto hash_data = reinterpret_cast<const uint64_t *>(hashes.GetData());

	PushNextBatchImpl(thread_id, count, hash_data);
}

void BloomFilterBuilder_Parallel::PushNextBatchImpl(size_t thread_id, int64_t num_rows, const uint64_t *hashes) {
	// partition IDs are calculated using the higher bits of the block ID
	// this ensures each block is contained entirely within a partition
	constexpr int kLogBlocksKeptTogether = 7;
	constexpr int kPrtnIdBitOffset = BloomFilterMasks::kLogNumMasks + 6 + kLogBlocksKeptTogether;

	int log_num_blocks = bloom_filter_->GetBlockedBloomFilter().LogNumBlocks();
	int log_num_prtns_max = std::max(0, log_num_blocks - kLogBlocksKeptTogether);
	int log_num_prtns_mod = std::min(log_num_prtns_, log_num_prtns_max);
	int num_prtns = 1 << log_num_prtns_mod;

	// get thread-local state
	ThreadLocalState &local_state = thread_local_states_[thread_id];
	local_state.partition_ranges.resize(num_prtns + 1);
	local_state.partitioned_hashes.resize(num_rows);
	local_state.unprocessed_partition_ids.resize(num_prtns);

	uint16_t *partition_ranges = local_state.partition_ranges.data();
	uint64_t *partitioned_hashes = local_state.partitioned_hashes.data();
	int *unprocessed_partition_ids = local_state.unprocessed_partition_ids.data();

	// bucket sort hashes by partition
	PartitionSort::Eval(
	    num_rows, num_prtns, partition_ranges,
	    [=](int64_t row_id) { return (hashes[row_id] >> kPrtnIdBitOffset) & (num_prtns - 1); },
	    [=](int64_t row_id, int output_pos) { partitioned_hashes[output_pos] = hashes[row_id]; });

	// build list of non-empty partitions
	int num_unprocessed_partitions = 0;
	for (int i = 0; i < num_prtns; ++i) {
		bool is_prtn_empty = (partition_ranges[i + 1] == partition_ranges[i]);
		if (!is_prtn_empty) {
			unprocessed_partition_ids[num_unprocessed_partitions++] = i;
		}
	}

	// process each partition under its lock
	while (num_unprocessed_partitions > 0) {
		int locked_prtn_id;
		int locked_prtn_id_pos;
		prtn_locks_.AcquirePartitionLock(thread_id, num_unprocessed_partitions, unprocessed_partition_ids,
		                                 /*limit_retries=*/false, /*max_retries=*/-1, &locked_prtn_id,
		                                 &locked_prtn_id_pos);

		// insert hashes for this partition (non-atomic, we hold the lock)
		int64_t start = partition_ranges[locked_prtn_id];
		int64_t end = partition_ranges[locked_prtn_id + 1];
		bloom_filter_->InsertHashes(end - start, partitioned_hashes + start);

		prtn_locks_.ReleasePartitionLock(locked_prtn_id);

		// remove processed partition from list
		if (locked_prtn_id_pos < num_unprocessed_partitions - 1) {
			unprocessed_partition_ids[locked_prtn_id_pos] = unprocessed_partition_ids[num_unprocessed_partitions - 1];
		}
		--num_unprocessed_partitions;
	}
}

void BloomFilterBuilder_Parallel::CleanUp() {
	thread_local_states_.clear();
	prtn_locks_.CleanUp();
}

vector<idx_t> BloomFilterBuilder_Parallel::BuiltCols() const {
	return bound_cols_;
}

//===--------------------------------------------------------------------===//
// BloomFilterBuilder (Legacy Wrapper)
//===--------------------------------------------------------------------===//

void BloomFilterBuilder::Begin(shared_ptr<BloomFilter> bf, const vector<idx_t> &bound_cols, size_t num_threads) {
	num_threads_ = num_threads;

	// estimate row count from bloom filter size
	// num_blocks = (num_rows * 8 bits) / 64 bits per block = num_rows / 8
	// so num_rows ≈ num_blocks * 8
	int64_t num_blocks = bf->GetBlockedBloomFilter().NumBlocks();
	idx_t estimated_rows = static_cast<idx_t>(num_blocks * 8);

	// auto-select strategy based on thread count AND estimated row count
	BloomFilterBuildStrategy strategy;
	if (num_threads > 1 && estimated_rows >= PARALLEL_BUILD_ROW_THRESHOLD) {
		strategy = BloomFilterBuildStrategy::PARALLEL;
	} else {
		strategy = BloomFilterBuildStrategy::SINGLE_THREADED;
	}

	impl_ = BloomFilterBuilderBase::Make(strategy);
	impl_->Begin(num_threads, std::move(bf), bound_cols);
}

void BloomFilterBuilder::PushNextBatch(DataChunk &chunk) const {
	// single-threaded interface uses thread_id = 0
	impl_->PushNextBatch(0, chunk);
}

void BloomFilterBuilder::PushNextBatch(size_t thread_id, DataChunk &chunk) const {
	impl_->PushNextBatch(thread_id, chunk);
}

void BloomFilterBuilder::CleanUp() {
	if (impl_) {
		impl_->CleanUp();
	}
}

vector<idx_t> BloomFilterBuilder::BuiltCols() const {
	return impl_->BuiltCols();
}

} // namespace duckdb
