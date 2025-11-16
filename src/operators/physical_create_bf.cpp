#include "physical_create_bf.hpp"
#include "dag.hpp"
#include "bloom_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include <iostream>

namespace duckdb {

PhysicalCreateBF::PhysicalCreateBF(const shared_ptr<BloomFilterOperation> bf_operation, vector<LogicalType> types,
                                   idx_t estimated_cardinality, vector<idx_t> bound_column_indices)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      bf_operation(bf_operation), is_probing_side(false), bound_column_indices(std::move(bound_column_indices)) {
}

string PhysicalCreateBF::GetName() const {
    return "CREATE_BF";
}

string PhysicalCreateBF::ToString(ExplainFormat format) const {
    string result = "CREATE_BF";
    result += " [" + std::to_string(bf_operation->build_columns.size()) + " filters]";
    return result;
}

unique_ptr<GlobalSinkState> PhysicalCreateBF::GetGlobalSinkState(ClientContext &context) const {
    auto state = make_uniq<PhysicalCreateBFGlobalSinkState>();
    
    // initialize bloom filters for each filter plan
    state->bloom_filters.reserve(bf_operation->build_columns.size());
    for (size_t i = 0; i < bf_operation->build_columns.size(); i++) {
        auto bf = make_shared_ptr<BloomFilter>();
        state->bloom_filters.push_back(bf);
    }
    
    return std::move(state);
}

unique_ptr<LocalSinkState> PhysicalCreateBF::GetLocalSinkState(ExecutionContext &context) const {
    return make_uniq<PhysicalCreateBFLocalSinkState>();
}

SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
    auto &lstate = input.local_state.Cast<PhysicalCreateBFLocalSinkState>();
    auto &gstate = input.global_state.Cast<PhysicalCreateBFGlobalSinkState>();

	Printer::Print("PhysicalCreateBF::Sink() called with chunk size: " + std::to_string(chunk.size()));

    if (chunk.size() > 0) {
        lock_guard<mutex> bf_guard(gstate.bf_lock);

        // process each bloom filter operation
        for (size_t i = 0; i < bf_operation->build_columns.size() && i < gstate.bloom_filters.size(); i++) {
            auto &bf = gstate.bloom_filters[i];

            if (bf) {
            	idx_t chunked_column_index = bound_column_indices[i];
                bf->Insert(chunk, {chunked_column_index});
            	printf("  inserted %llu rows into bloom filter %zu (chunk column %llu)\n",
						 chunk.size(), i, chunked_column_index);
            }
        }

    }

    return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateBF::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
    // no local state combining needed
    return SinkCombineResultType::FINISHED;
}

SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<PhysicalCreateBFGlobalSinkState>();
    
    lock_guard<mutex> bf_guard(gstate.bf_lock);
    
    // initialize and finalize bloom filters
    for (auto &bf : gstate.bloom_filters) {
        if (bf) {
            // Initialize with a reasonable size based on estimated cardinality
            bf->Initialize(context, estimated_cardinality);
            bf->finalized_ = true;
        }
    }
    
    // return READY with no output - we only build the filter, don't output data
    return SinkFinalizeType::READY;
}

vector<shared_ptr<BloomFilter>> PhysicalCreateBF::GetBloomFilters() const {
    // access the sink state to get bloom filters
    if (sink_state) {
        auto &gstate = sink_state->Cast<PhysicalCreateBFGlobalSinkState>();
        lock_guard<mutex> bf_guard(gstate.bf_lock);
        return gstate.bloom_filters;
    }
    return {};
}

} // namespace duckdb