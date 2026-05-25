#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/unordered_map.hpp"
#include <atomic>
#include <mutex>

namespace duckdb {

// per-query registry mapping probe_table_idx -> shared empty-flag.
// used to short-circuit CREATE_FILTER operators whose probe side is known empty
// (e.g. a sibling CREATE_FILTER targeting the same probe finalized with an empty
// BF, or a PROBE_FILTER detected an empty build BF).
class ProbeEmptyRegistry : public ClientContextState {
public:
	mutex reg_lock;
	unordered_map<idx_t, shared_ptr<std::atomic<bool>>> flags;

	shared_ptr<std::atomic<bool>> GetOrCreate(idx_t probe_table_idx) {
		lock_guard<mutex> g(reg_lock);
		auto it = flags.find(probe_table_idx);
		if (it != flags.end()) {
			return it->second;
		}
		auto f = make_shared_ptr<std::atomic<bool>>(false);
		flags[probe_table_idx] = f;
		return f;
	}

	void QueryEnd(ClientContext &ctx) override {
		{
			lock_guard<mutex> g(reg_lock);
			flags.clear();
		}
		ctx.registered_state->Remove("robust_probe_empty");
	}
};

inline shared_ptr<ProbeEmptyRegistry> GetProbeEmptyRegistry(ClientContext &context) {
	return context.registered_state->GetOrCreate<ProbeEmptyRegistry>("robust_probe_empty");
}

} // namespace duckdb
