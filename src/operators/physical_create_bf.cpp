#include "physical_create_bf.hpp"
#include "dag.hpp"
#include "bloom_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include <iostream>
#include <duckdb/parallel/meta_pipeline.hpp>
#include <duckdb/parallel/thread_context.hpp>

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

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

CreateBFGlobalSinkState::CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op)
	: op(op){
	total_data = make_uniq<ColumnDataCollection>(context, op.types);
}

 CreateBFLocalSinkState::CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op)
	 : client_context(context){
	local_data = make_uniq<ColumnDataCollection>(client_context, op.types);
}



SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	CreateBFLocalSinkState &local_state = input.local_state.Cast<CreateBFLocalSinkState>();
	local_state.local_data->Append(chunk);
	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateBF::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	CreateBFGlobalSinkState &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();
	CreateBFLocalSinkState &local_state = input.local_state.Cast<CreateBFLocalSinkState>();
	lock_guard<mutex> lock(gstate.glock);
	gstate.local_data_collections.emplace_back(std::move(local_state.local_data));
    return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//
struct SchedulerThread {
#ifndef DUCKDB_NO_THREADS
	explicit SchedulerThread(unique_ptr<std::thread> thread_p) : internal_thread(std::move(thread_p)) {
	}

	unique_ptr<std::thread> internal_thread;
#endif
};

class CreateBFFinalizeTask : public ExecutorTask {
public:
	CreateBFFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, CreateBFGlobalSinkState &sink_p,
		idx_t chunk_idx_from_p, idx_t chunk_idx_to_p, size_t num_threads)
			: ExecutorTask(context), event(std::move(event_p)), sink(sink_p), chunk_idx_from(chunk_idx_from_p),
			  chunk_idx_to(chunk_idx_to_p) {

	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ThreadContext tcontext(this->executor.context);
		tcontext.profiler.StartOperator(&sink.op);

		size_t thread_id = 0;
		for (idx_t i = chunk_idx_from; i < chunk_idx_to; i++) {
			DataChunk chunk;
			sink.total_data->InitializeScanChunk(chunk);
			sink.total_data->FetchChunk(i, chunk);
			for (shared_ptr<BloomFilterBuilder> &bf_builder : sink.bf_builders) {
				vector<idx_t> cols = bf_builder->BuiltCols();
				Vector hashes(LogicalType::HASH);
				VectorOperations::Hash(chunk.data[cols[0]], hashes, chunk.size());
				for (int i = 1; i < cols.size(); i++) {
					VectorOperations::CombineHash(hashes, chunk.data[cols[i]], chunk.size());
				}
				if (hashes.GetVectorType() == VectorType::CONSTANT_VECTOR) {
					hashes.Flatten(chunk.size());
				}
				bf_builder->PushNextBatch(chunk.size(), reinterpret_cast<hash_t *>(hashes.GetData()));

			}
		}

		event->FinishTask();
		tcontext.profiler.EndOperator(nullptr);
		this->executor.Flush(tcontext);
		return TaskExecutionResult::TASK_FINISHED;
	}

private:
	shared_ptr<Event> event;
	CreateBFGlobalSinkState &sink;
	idx_t chunk_idx_from;
	idx_t chunk_idx_to;
};



SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
    auto &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();
}

unique_ptr<GlobalSinkState> PhysicalCreateBF::GetGlobalSinkState(ClientContext &context) const {
	auto state = make_uniq<CreateBFGlobalSinkState>(context, *this);

	// initialize bloom filters
	state->bloom_filters.reserve(bf_operation->build_columns.size());
	for (size_t i = 0; i < bf_operation->build_columns.size(); i++) {
		auto bf = make_shared_ptr<BloomFilter>();
		state->bloom_filters.push_back(bf);
	}

	return state;
}

unique_ptr<LocalSinkState> PhysicalCreateBF::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CreateBFLocalSinkState>();
}

vector<shared_ptr<BloomFilter>> PhysicalCreateBF::GetBloomFilters() const {
    // access the sink state to get bloom filters
    if (sink_state) {
        auto &gstate = sink_state->Cast<CreateBFGlobalSinkState>();
        lock_guard<mutex> bf_guard(gstate.bf_lock);
        return gstate.bloom_filters;
    }
    return {};
}

unique_ptr<GlobalSourceState> PhysicalCreateBF::GetGlobalSourceState(ClientContext &context) const {
	auto state = make_uniq<CreateBFGlobalSinkState>(context, *this);
	return state;
}



unique_ptr<LocalSourceState> PhysicalCreateBF::GetLocalSourceState(
	ExecutionContext &context, GlobalSourceState &gstate) const {
	return make_uniq<PhysicalCreateBFLocalSourceState>();
}

SourceResultType PhysicalCreateBF::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<CreateBFGlobalSinkState>();
	auto &state = input.global_state.Cast<PhysicalCreateBFGlobalSourceState>();

	gstate.collected_data->Scan(state.scan_state, chunk);

	if (chunk.size() == 0) {
		return SourceResultType::FINISHED;
	}

	return SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalCreateBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

	auto &state = meta_pipeline.GetState();

	// make this operator source of the pipeline
	state.SetPipelineSource(current, *this);

	if (this_pipeline == nullptr) {
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(children[0].get());
	} else {
		current.AddDependency(this_pipeline);
	}
}

void PhysicalCreateBF::BuildPipelinesFromRelated(Pipeline &current,
												   MetaPipeline &meta_pipeline) {
	op_state.reset();

	if (this_pipeline == nullptr) {
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(children[0].get());
	} else {
		// add dependency so current pipeline waits for this CREATE_BF
		current.AddDependency(this_pipeline);
	}
}

} // namespace duckdb