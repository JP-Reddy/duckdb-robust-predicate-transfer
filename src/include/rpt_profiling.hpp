#pragma once

#include "duckdb/main/client_context_state.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"
#include <chrono>
#include <atomic>
#include <mutex>
#include <vector>
#include <algorithm>
#include <map>

namespace duckdb {

struct CreateBFStats {
	idx_t sequence_number = 0;
	idx_t build_table_idx = 0;
	vector<idx_t> probe_table_indices;
	bool is_forward_pass = false;
	std::atomic<idx_t> rows_materialized {0};
	std::atomic<int64_t> sink_time_us {0};
	std::atomic<int64_t> finalize_time_us {0};
	std::atomic<int64_t> source_time_us {0};
};

struct UseBFStats {
	idx_t sequence_number = 0;
	idx_t build_table_idx = 0;
	idx_t probe_table_idx = 0;
	bool is_forward_pass = false;
	std::atomic<idx_t> rows_in {0};
	std::atomic<idx_t> rows_out {0};
	std::atomic<int64_t> probe_time_us {0};
};

// RAII timer that adds elapsed microseconds to an atomic counter
struct ScopedTimer {
	std::atomic<int64_t> &target;
	std::chrono::high_resolution_clock::time_point start;

	explicit ScopedTimer(std::atomic<int64_t> &target) : target(target) {
		start = std::chrono::high_resolution_clock::now();
	}
	~ScopedTimer() {
		auto end = std::chrono::high_resolution_clock::now();
		target.fetch_add(std::chrono::duration_cast<std::chrono::microseconds>(end - start).count(),
		                 std::memory_order_relaxed);
	}
};

class RPTProfilingState : public ClientContextState {
public:
	explicit RPTProfilingState(bool enabled) : enabled(enabled) {
	}

	bool enabled;
	int64_t optimizer_time_us = 0;

	// table index -> resolved table name (populated at optimizer time)
	std::map<idx_t, string> table_names;

	mutex stats_lock;
	vector<shared_ptr<CreateBFStats>> create_bf_stats;
	vector<shared_ptr<UseBFStats>> use_bf_stats;

	string GetName(idx_t table_idx) const {
		auto it = table_names.find(table_idx);
		if (it != table_names.end()) {
			return it->second;
		}
		return "table_" + std::to_string(table_idx);
	}

	shared_ptr<CreateBFStats> RegisterCreateBF(idx_t build_table_idx, const vector<ColumnBinding> &probe_columns,
	                                           idx_t sequence_number, bool is_forward_pass) {
		lock_guard<mutex> lock(stats_lock);
		auto stats = make_shared_ptr<CreateBFStats>();
		stats->sequence_number = sequence_number;
		stats->build_table_idx = build_table_idx;
		stats->is_forward_pass = is_forward_pass;
		// extract unique probe table indices from probe columns
		for (const auto &col : probe_columns) {
			if (stats->probe_table_indices.empty() || stats->probe_table_indices.back() != col.table_index) {
				// check if already present
				bool found = false;
				for (auto idx : stats->probe_table_indices) {
					if (idx == col.table_index) {
						found = true;
						break;
					}
				}
				if (!found) {
					stats->probe_table_indices.push_back(col.table_index);
				}
			}
		}
		create_bf_stats.push_back(stats);
		return stats;
	}

	shared_ptr<UseBFStats> RegisterUseBF(idx_t build_table_idx, idx_t probe_table_idx, idx_t sequence_number,
	                                     bool is_forward_pass) {
		lock_guard<mutex> lock(stats_lock);
		auto stats = make_shared_ptr<UseBFStats>();
		stats->sequence_number = sequence_number;
		stats->build_table_idx = build_table_idx;
		stats->probe_table_idx = probe_table_idx;
		stats->is_forward_pass = is_forward_pass;
		use_bf_stats.push_back(stats);
		return stats;
	}

	void QueryEnd(ClientContext &context) override {
		if (!enabled) {
			return;
		}
		PrintSummary();
		context.registered_state->Remove("rpt_profiling");
	}

	void PrintSummary() {
		Printer::Print("\n=== RPT PROFILING ===");
		Printer::PrintF("Optimizer: %lld us", (long long)optimizer_time_us);

		// build a combined list sorted by sequence_number
		struct StatsEntry {
			idx_t seq;
			bool is_create;
			size_t idx;
		};
		vector<StatsEntry> entries;
		for (size_t i = 0; i < create_bf_stats.size(); i++) {
			entries.push_back({create_bf_stats[i]->sequence_number, true, i});
		}
		for (size_t i = 0; i < use_bf_stats.size(); i++) {
			entries.push_back({use_bf_stats[i]->sequence_number, false, i});
		}
		std::sort(entries.begin(), entries.end(),
		          [](const StatsEntry &a, const StatsEntry &b) { return a.seq < b.seq; });

		// per-pass accumulators
		int64_t fwd_rows_in = 0, fwd_rows_out = 0, fwd_probe_us = 0;
		int64_t bwd_rows_in = 0, bwd_rows_out = 0, bwd_probe_us = 0;
		int64_t total_sink_us = 0, total_source_us = 0, total_finalize_us = 0;

		Printer::Print("");
		for (auto &e : entries) {
			if (e.is_create) {
				auto &s = create_bf_stats[e.idx];
				string pass = s->is_forward_pass ? "FWD" : "BWD";
				string probe_names;
				for (size_t pi = 0; pi < s->probe_table_indices.size(); pi++) {
					if (pi > 0)
						probe_names += ",";
					probe_names += GetName(s->probe_table_indices[pi]);
				}
				if (probe_names.empty())
					probe_names = "?";
				Printer::PrintF(
				    "CREATE_BF [%s]: [build=%s -> probe=%s] %llu rows, sink=%lldus, finalize=%lldus, source=%lldus",
				    pass.c_str(), GetName(s->build_table_idx).c_str(), probe_names.c_str(),
				    (unsigned long long)s->rows_materialized.load(), (long long)s->sink_time_us.load(),
				    (long long)s->finalize_time_us.load(), (long long)s->source_time_us.load());
				total_sink_us += s->sink_time_us.load();
				total_source_us += s->source_time_us.load();
				total_finalize_us += s->finalize_time_us.load();
			} else {
				auto &s = use_bf_stats[e.idx];
				string pass = s->is_forward_pass ? "FWD" : "BWD";
				idx_t ri = s->rows_in.load();
				idx_t ro = s->rows_out.load();
				double sel = ri > 0 ? 100.0 * (double)ro / ri : 0.0;
				Printer::PrintF("USE_BF    [%s]: [build=%s, probe=%s] in=%llu, out=%llu, sel=%.1f%%, probe=%lldus",
				                pass.c_str(), GetName(s->build_table_idx).c_str(),
				                GetName(s->probe_table_idx).c_str(), (unsigned long long)ri, (unsigned long long)ro,
				                sel, (long long)s->probe_time_us.load());
				if (s->is_forward_pass) {
					fwd_rows_in += ri;
					fwd_rows_out += ro;
					fwd_probe_us += s->probe_time_us.load();
				} else {
					bwd_rows_in += ri;
					bwd_rows_out += ro;
					bwd_probe_us += s->probe_time_us.load();
				}
			}
		}

		Printer::Print("\nTotals:");
		Printer::PrintF("  sink: %lld us", (long long)total_sink_us);
		Printer::PrintF("  source: %lld us", (long long)total_source_us);
		Printer::PrintF("  finalize (BF build): %lld us", (long long)total_finalize_us);

		auto print_pass_stats = [](const char *label, int64_t rows_in, int64_t rows_out, int64_t probe_us) {
			if (rows_in > 0) {
				double filtered_pct = 100.0 * (1.0 - (double)rows_out / rows_in);
				Printer::PrintF("  %s probe: %lld us, filtered: %lld / %lld rows (%.1f%% removed)", label,
				                (long long)probe_us, (long long)(rows_in - rows_out), (long long)rows_in,
				                filtered_pct);
			}
		};

		print_pass_stats("forward", fwd_rows_in, fwd_rows_out, fwd_probe_us);
		print_pass_stats("backward", bwd_rows_in, bwd_rows_out, bwd_probe_us);

		int64_t total_rows_in = fwd_rows_in + bwd_rows_in;
		int64_t total_rows_out = fwd_rows_out + bwd_rows_out;
		if (total_rows_in > 0) {
			double filtered_pct = 100.0 * (1.0 - (double)total_rows_out / total_rows_in);
			Printer::PrintF("  total probe: %lld us, filtered: %lld / %lld rows (%.1f%% removed)",
			                (long long)(fwd_probe_us + bwd_probe_us), (long long)(total_rows_in - total_rows_out),
			                (long long)total_rows_in, filtered_pct);
		}
		Printer::Print("=== END RPT PROFILING ===\n");
	}
};

inline shared_ptr<RPTProfilingState> GetRPTProfilingState(ClientContext &context) {
	Value val;
	auto result = context.TryGetCurrentSetting("rpt_profiling", val);
	if (result && val.GetValue<bool>()) {
		return context.registered_state->GetOrCreate<RPTProfilingState>("rpt_profiling", true);
	}
	return nullptr;
}

} // namespace duckdb
