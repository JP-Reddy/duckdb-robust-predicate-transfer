// Ported from Apache Arrow Acero's BlockedBloomFilter implementation.
// Licensed under the Apache License, Version 2.0.
// All Arrow dependencies replaced with DuckDB/stdlib equivalents.

#pragma once

#include "duckdb/common/allocator.hpp"
#include "duckdb/common/assert.hpp"

#include <atomic>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <random>

namespace duckdb {

// rotate left for 64-bit
inline uint64_t Rotl64(uint64_t x, int r) {
	return (x << r) | (x >> (64 - r));
}

// unaligned load via memcpy (optimized away by compiler)
template <typename T>
inline T SafeLoadAs(const uint8_t *ptr) {
	T val;
	memcpy(&val, ptr, sizeof(T));
	return val;
}

// popcount for a byte buffer
inline int64_t CountSetBits(const uint8_t *data, int64_t num_bits) {
	int64_t count = 0;
	int64_t num_full_words = num_bits / 64;
	const auto *words = reinterpret_cast<const uint64_t *>(data);
	for (int64_t i = 0; i < num_full_words; ++i) {
		count += __builtin_popcountll(words[i]);
	}
	// remaining bits
	int64_t remaining = num_bits - num_full_words * 64;
	if (remaining > 0) {
		const uint8_t *tail = data + num_full_words * 8;
		for (int64_t i = 0; i < (remaining + 7) / 8; ++i) {
			count += __builtin_popcount(tail[i]);
		}
	}
	return count;
}

inline bool GetBit(const uint8_t *bits, int64_t i) {
	return (bits[i / 8] >> (i % 8)) & 1;
}

inline void SetBit(uint8_t *bits, int64_t i) {
	bits[i / 8] |= (1 << (i % 8));
}

inline int64_t CeilDiv(int64_t a, int64_t b) {
	return (a + b - 1) / b;
}

inline int Log2Floor(int64_t n) {
	D_ASSERT(n > 0);
	return 63 - __builtin_clzll(static_cast<uint64_t>(n));
}

// ceil(log2(n)) — returns the number of bits needed to represent n values
inline int Log2Ceil(int64_t n) {
	if (n <= 1) return 0;
	return Log2Floor(n - 1) + 1;
}

// pre-generated bit masks from a 64-bit word, used to map hash bits to a bloom filter mask.
// masks are 57 bits long so they can be accessed at arbitrary bit offsets via unaligned 64-bit load.
struct BloomFilterMasks {
	BloomFilterMasks() {
		std::seed_seq seed{0, 0, 0, 0, 0, 0, 0, 0};
		std::mt19937 re(seed);
		std::uniform_int_distribution<uint64_t> rd;
		auto random = [&re, &rd](int min_value, int max_value) {
			return min_value + static_cast<int>(rd(re) % (max_value - min_value + 1));
		};

		memset(masks_, 0, kTotalBytes);

		// prepare the first mask
		int num_bits_set = random(kMinBitsSet, kMaxBitsSet);
		for (int i = 0; i < num_bits_set; ++i) {
			for (;;) {
				int bit_pos = random(0, kBitsPerMask - 1);
				if (!GetBit(masks_, bit_pos)) {
					SetBit(masks_, bit_pos);
					break;
				}
			}
		}

		int64_t num_bits_total = kNumMasks + kBitsPerMask - 1;

		for (int64_t i = kBitsPerMask; i < num_bits_total; ++i) {
			int bit_leaving = GetBit(masks_, i - kBitsPerMask) ? 1 : 0;

			if (bit_leaving == 1 && num_bits_set == kMinBitsSet) {
				SetBit(masks_, i);
				continue;
			}

			if (bit_leaving == 0 && num_bits_set == kMaxBitsSet) {
				continue;
			}

			if (random(0, kBitsPerMask * 2 - 1) < kMinBitsSet + kMaxBitsSet) {
				SetBit(masks_, i);
				if (bit_leaving == 0) {
					++num_bits_set;
				}
			} else {
				if (bit_leaving == 1) {
					--num_bits_set;
				}
			}
		}
	}

	inline uint64_t mask(int bit_offset) const {
		// assumes little-endian (x86 and ARM are both LE)
		return (SafeLoadAs<uint64_t>(masks_ + bit_offset / 8) >> (bit_offset % 8)) & kFullMask;
	}

	static constexpr int kBitsPerMask = 57;
	static constexpr uint64_t kFullMask = (1ULL << kBitsPerMask) - 1;
	static constexpr int kMinBitsSet = 4;
	static constexpr int kMaxBitsSet = 5;
	static constexpr int kLogNumMasks = 10;
	static constexpr int kNumMasks = 1 << kLogNumMasks;
	static constexpr int kTotalBytes = (kNumMasks + 64) / 8;

	uint8_t masks_[kTotalBytes];
};

// blocked bloom filter with 64-bit blocks and power-of-2 count.
// 1 cache access per key, 8 bits per key, with folding for sparse filters.
class BlockedBloomFilter {
public:
	BlockedBloomFilter() : log_num_blocks_(0), num_blocks_(0), blocks_(nullptr) {}

	void CreateEmpty(int64_t num_rows_to_insert) {
		constexpr int64_t min_num_bits_per_key = 8;
		constexpr int64_t min_num_bits = 512;
		int64_t desired_num_bits =
		    std::max(min_num_bits, num_rows_to_insert * min_num_bits_per_key);
		int log_num_bits = Log2Ceil(desired_num_bits);

		log_num_blocks_ = log_num_bits - 6;
		num_blocks_ = 1LL << log_num_blocks_;

		int64_t buffer_size = num_blocks_ * static_cast<int64_t>(sizeof(uint64_t));
		buf_ = Allocator::DefaultAllocator().Allocate(buffer_size);
		blocks_ = reinterpret_cast<uint64_t *>(buf_.get());
		memset(blocks_, 0, buffer_size);
	}

	inline bool Find(uint64_t hash) const {
		uint64_t m = ComputeMask(hash);
		uint64_t b = blocks_[BlockId(hash)];
		return (b & m) == m;
	}

	inline void Insert(uint64_t hash) {
		uint64_t m = ComputeMask(hash);
		blocks_[BlockId(hash)] |= m;
	}

	// atomic insert for parallel building (fetch_or on the 64-bit block)
	inline void InsertAtomic(uint64_t hash) {
		uint64_t m = ComputeMask(hash);
		auto *atomic_blocks = reinterpret_cast<std::atomic<uint64_t> *>(blocks_);
		atomic_blocks[BlockId(hash)].fetch_or(m, std::memory_order_relaxed);
	}

	// batch insert (scalar loop)
	void Insert(int64_t num_rows, const uint64_t *hashes) {
		for (int64_t i = 0; i < num_rows; ++i) {
			Insert(hashes[i]);
		}
	}

	// atomic batch insert for parallel building
	void InsertAtomic(int64_t num_rows, const uint64_t *hashes) {
		for (int64_t i = 0; i < num_rows; ++i) {
			InsertAtomic(hashes[i]);
		}
	}

	// batch find with prefetching, results as bit vector
	void Find(int64_t num_rows, const uint64_t *hashes, uint8_t *result_bit_vector) const {
		int64_t num_processed = 0;
		uint64_t bits = 0ULL;

		if (UsePrefetch()) {
			constexpr int kPrefetchIterations = 16;
			for (int64_t i = 0; i < num_rows - kPrefetchIterations; ++i) {
				__builtin_prefetch(blocks_ + BlockId(hashes[i + kPrefetchIterations]));
				uint64_t result = Find(hashes[i]) ? 1ULL : 0ULL;
				bits |= result << (i & 63);
				if ((i & 63) == 63) {
					reinterpret_cast<uint64_t *>(result_bit_vector)[i / 64] = bits;
					bits = 0ULL;
				}
			}
			num_processed = num_rows - kPrefetchIterations;
		}

		for (int64_t i = num_processed; i < num_rows; ++i) {
			uint64_t result = Find(hashes[i]) ? 1ULL : 0ULL;
			bits |= result << (i & 63);
			if ((i & 63) == 63) {
				reinterpret_cast<uint64_t *>(result_bit_vector)[i / 64] = bits;
				bits = 0ULL;
			}
		}

		// flush remaining bits
		for (int i = 0; i < CeilDiv(num_rows % 64, 8); ++i) {
			result_bit_vector[num_rows / 64 * 8 + i] = static_cast<uint8_t>(bits >> (i * 8));
		}
	}

	// compress sparse filters by OR-ing halves until >25% density
	void Fold() {
		for (;;) {
			constexpr int log_num_blocks_min = 4;
			if (log_num_blocks_ <= log_num_blocks_min) {
				break;
			}

			int64_t num_bits = num_blocks_ * 64;
			int64_t num_bits_set = CountSetBits(reinterpret_cast<const uint8_t *>(blocks_), num_bits);

			if (4 * num_bits_set >= num_bits) {
				break;
			}

			int num_folds = 1;
			while ((log_num_blocks_ - num_folds) > log_num_blocks_min &&
			       (4 * num_bits_set) < (num_bits >> num_folds)) {
				++num_folds;
			}

			SingleFold(num_folds);
		}
	}

	bool IsEmpty() const {
		return blocks_ == nullptr || num_blocks_ == 0;
	}

	int64_t NumBlocks() const {
		return num_blocks_;
	}

	int LogNumBlocks() const {
		return log_num_blocks_;
	}

	// direct access to blocks for partition-based parallel building
	uint64_t *Blocks() {
		return blocks_;
	}

private:
	inline uint64_t ComputeMask(uint64_t hash) const {
		int mask_id = static_cast<int>(hash & (BloomFilterMasks::kNumMasks - 1));
		uint64_t result = masks_.mask(mask_id);
		int rotation = (hash >> BloomFilterMasks::kLogNumMasks) & 63;
		result = Rotl64(result, rotation);
		return result;
	}

	inline int64_t BlockId(uint64_t hash) const {
		return (hash >> (BloomFilterMasks::kLogNumMasks + 6)) & (num_blocks_ - 1);
	}

	bool UsePrefetch() const {
		return num_blocks_ * static_cast<int64_t>(sizeof(uint64_t)) > kPrefetchLimitBytes;
	}

	void SingleFold(int num_folds) {
		int64_t num_slices = 1LL << num_folds;
		int64_t num_slice_blocks = (num_blocks_ >> num_folds);
		uint64_t *target_slice = blocks_;

		for (int64_t slice = 1; slice < num_slices; ++slice) {
			const uint64_t *source_slice = blocks_ + slice * num_slice_blocks;
			for (int64_t i = 0; i < num_slice_blocks; ++i) {
				target_slice[i] |= source_slice[i];
			}
		}

		log_num_blocks_ -= num_folds;
		num_blocks_ = 1LL << log_num_blocks_;
	}

	static constexpr int64_t kPrefetchLimitBytes = 256 * 1024;

	static BloomFilterMasks masks_;

	int log_num_blocks_;
	int64_t num_blocks_;

	AllocatedData buf_;
	uint64_t *blocks_;
};

} // namespace duckdb
