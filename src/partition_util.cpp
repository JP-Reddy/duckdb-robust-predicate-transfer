// Ported from Apache Arrow Acero's partition_util implementation.
// Licensed under the Apache License, Version 2.0.

#include "partition_util.hpp"

#include <random>

namespace duckdb {

PartitionLocks::PartitionLocks() : num_prtns_(0), locks_(nullptr), rngs_(nullptr) {
}

PartitionLocks::~PartitionLocks() {
	CleanUp();
}

void PartitionLocks::Init(size_t num_threads, int num_prtns) {
	num_prtns_ = num_prtns;
	locks_.reset(new PartitionLock[num_prtns]);
	rngs_.reset(new std::mt19937[num_threads]);

	// initialize all locks to unlocked
	for (int i = 0; i < num_prtns; ++i) {
		locks_[i].lock.store(false);
	}

	// seed each thread's RNG differently
	std::mt19937 seed_gen(0);
	std::uniform_int_distribution<uint32_t> seed_dist;
	for (size_t i = 0; i < num_threads; ++i) {
		rngs_[i].seed(seed_dist(seed_gen));
	}
}

void PartitionLocks::CleanUp() {
	locks_.reset();
	rngs_.reset();
	num_prtns_ = 0;
}

std::atomic<bool> *PartitionLocks::LockPtr(int prtn_id) {
	D_ASSERT(locks_);
	D_ASSERT(prtn_id >= 0 && prtn_id < num_prtns_);
	return &locks_[prtn_id].lock;
}

int PartitionLocks::RandomInt(size_t thread_id, int num_values) {
	return std::uniform_int_distribution<int> {0, num_values - 1}(rngs_[thread_id]);
}

bool PartitionLocks::AcquirePartitionLock(size_t thread_id, int num_prtns_to_try, const int *prtns_to_try,
                                          bool limit_retries, int max_retries, int *locked_prtn_id,
                                          int *locked_prtn_id_pos) {
	int trial = 0;

	while (!limit_retries || trial <= max_retries) {
		// randomly pick a partition to try (reduces convoy effects)
		int prtn_id_pos = RandomInt(thread_id, num_prtns_to_try);
		int prtn_id = prtns_to_try[prtn_id_pos];

		std::atomic<bool> *lock = LockPtr(prtn_id);

		// try to acquire the lock with compare_exchange
		bool expected = false;
		if (lock->compare_exchange_weak(expected, true, std::memory_order_acquire)) {
			*locked_prtn_id = prtn_id;
			*locked_prtn_id_pos = prtn_id_pos;
			return true;
		}

		++trial;
	}

	*locked_prtn_id = -1;
	*locked_prtn_id_pos = -1;
	return false;
}

void PartitionLocks::ReleasePartitionLock(int prtn_id) {
	D_ASSERT(prtn_id >= 0 && prtn_id < num_prtns_);
	std::atomic<bool> *lock = LockPtr(prtn_id);
	lock->store(false, std::memory_order_release);
}

} // namespace duckdb
