// Ported from Apache Arrow Acero's partition_util implementation.
// Licensed under the Apache License, Version 2.0.
// All Arrow dependencies replaced with DuckDB/stdlib equivalents.

#pragma once

#include "duckdb/common/assert.hpp"

#include <atomic>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <random>
#include <vector>

namespace duckdb {

// Bucket sort rows on partition IDs in O(num_rows) time.
//
// This is used to group hashes by the partition (block region) they belong to,
// so that each partition can be processed under a single lock.
class PartitionSort {
public:
	// Bucket sort rows on partition ids.
	//
	// prtn_ranges must have at least num_prtns + 1 elements.
	// After return, prtn_ranges[i] contains the total number of elements
	// in partitions 0 through i. prtn_ranges[0] will be 0.
	//
	// prtn_id_fn: Takes row_id, returns partition_id (0 to num_prtns-1)
	// output_fn: Takes (row_id, output_position), should store the row at output_position
	//
	// Example:
	//   in_arr: [5, 7, 2, 3, 5, 4]
	//   num_prtns: 3
	//   prtn_id_fn: [](int64_t row_id) { return in_arr[row_id] / 3; }
	//   output_fn: [](int64_t row_id, int pos) { sorted_row_ids[pos] = row_id; }
	//
	//   After execution:
	//   sorted_row_ids: [2, 0, 3, 4, 5, 1]
	//   prtn_ranges: [0, 1, 5, 6]
	//
	template <typename PRTN_ID_FN, typename OUTPUT_FN>
	static void Eval(int64_t num_rows, int num_prtns, uint16_t *prtn_ranges,
	                 PRTN_ID_FN prtn_id_fn, OUTPUT_FN output_fn) {
		D_ASSERT(num_rows > 0 && num_rows <= (1 << 15));
		D_ASSERT(num_prtns >= 1 && num_prtns <= (1 << 15));

		memset(prtn_ranges, 0, (num_prtns + 1) * sizeof(uint16_t));

		// count elements per partition
		for (int64_t i = 0; i < num_rows; ++i) {
			int prtn_id = static_cast<int>(prtn_id_fn(i));
			++prtn_ranges[prtn_id + 1];
		}

		// convert counts to cumulative ranges
		// after this: prtn_ranges[i+1] = start position for partition i
		uint16_t sum = 0;
		for (int i = 0; i < num_prtns; ++i) {
			uint16_t sum_next = sum + prtn_ranges[i + 1];
			prtn_ranges[i + 1] = sum;
			sum = sum_next;
		}

		// place each element at its sorted position
		for (int64_t i = 0; i < num_rows; ++i) {
			int prtn_id = static_cast<int>(prtn_id_fn(i));
			int pos = prtn_ranges[prtn_id + 1]++;
			output_fn(i, pos);
		}
	}
};

// A control for synchronizing threads on a partitionable workload.
//
// Each partition has its own lock. Threads randomly try to acquire locks
// to avoid convoy effects.
class PartitionLocks {
public:
	PartitionLocks();
	~PartitionLocks();

	// Initialize the control. Must be called before use.
	//
	// num_threads: Maximum number of threads that will access the partitions
	// num_prtns: Number of partitions to synchronize
	void Init(size_t num_threads, int num_prtns);

	// Clean up the control. Should not be used after this call.
	void CleanUp();

	// Acquire a partition to work on.
	//
	// thread_id: Index of the thread trying to acquire
	// num_prtns_to_try: Length of prtns_to_try array
	// prtns_to_try: Array of partition IDs that still have remaining work
	// limit_retries: If false, spin forever until success
	// max_retries: Max attempts before returning false (if limit_retries=true)
	// locked_prtn_id: Output - the ID of the partition locked
	// locked_prtn_id_pos: Output - the index in prtns_to_try of the locked partition
	//
	// Returns true if a partition was locked, false if max_retries exceeded.
	bool AcquirePartitionLock(size_t thread_id, int num_prtns_to_try,
	                          const int *prtns_to_try, bool limit_retries,
	                          int max_retries, int *locked_prtn_id,
	                          int *locked_prtn_id_pos);

	// Release a partition so other threads can work on it.
	void ReleasePartitionLock(int prtn_id);

	// Process all partitions, acquiring locks as needed.
	//
	// thread_id: Index of the calling thread
	// temp_unprocessed_prtns: Scratch buffer with space for num_prtns elements
	// is_prtn_empty_fn: Returns true if partition should be skipped
	// process_prtn_fn: Called for each partition after acquiring its lock
	template <typename IS_PRTN_EMPTY_FN, typename PROCESS_PRTN_FN>
	void ForEachPartition(size_t thread_id, int *temp_unprocessed_prtns,
	                      IS_PRTN_EMPTY_FN is_prtn_empty_fn,
	                      PROCESS_PRTN_FN process_prtn_fn) {
		int num_unprocessed = 0;
		for (int i = 0; i < num_prtns_; ++i) {
			if (!is_prtn_empty_fn(i)) {
				temp_unprocessed_prtns[num_unprocessed++] = i;
			}
		}

		while (num_unprocessed > 0) {
			int locked_prtn_id;
			int locked_prtn_id_pos;
			AcquirePartitionLock(thread_id, num_unprocessed, temp_unprocessed_prtns,
			                     /*limit_retries=*/false, /*max_retries=*/-1,
			                     &locked_prtn_id, &locked_prtn_id_pos);

			// RAII lock release
			struct AutoRelease {
				PartitionLocks *locks;
				int prtn_id;
				~AutoRelease() { locks->ReleasePartitionLock(prtn_id); }
			} auto_release{this, locked_prtn_id};

			process_prtn_fn(locked_prtn_id);

			// remove processed partition from list
			if (locked_prtn_id_pos < num_unprocessed - 1) {
				temp_unprocessed_prtns[locked_prtn_id_pos] =
				    temp_unprocessed_prtns[num_unprocessed - 1];
			}
			--num_unprocessed;
		}
	}

	int NumPartitions() const { return num_prtns_; }

private:
	std::atomic<bool> *LockPtr(int prtn_id);
	int RandomInt(size_t thread_id, int num_values);

	// cache-line padded lock to avoid false sharing
	struct alignas(64) PartitionLock {
		std::atomic<bool> lock{false};
	};

	int num_prtns_;
	std::unique_ptr<PartitionLock[]> locks_;
	std::unique_ptr<std::mt19937[]> rngs_;
};

} // namespace duckdb
