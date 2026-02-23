#include "physical_create_bf.hpp"
#include "bloom_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"
#include "debug_utils.hpp"
#include "rpt_profiling.hpp"
#include <iostream>
#include <unordered_set>
#include <duckdb/parallel/meta_pipeline.hpp>
#include <duckdb/parallel/thread_context.hpp>

namespace duckdb {

PhysicalCreateBF::PhysicalCreateBF(PhysicalPlan &physical_plan, const shared_ptr<BloomFilterOperation> bf_operation, vector<LogicalType> types,
                                   idx_t estimated_cardinality, vector<idx_t> bound_column_indices)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      bf_operation(bf_operation), is_probing_side(false), bound_column_indices(std::move(bound_column_indices)) {
	// create bloom filter for each build column, keyed by ColumnBinding
	for (size_t i = 0; i < bf_operation->build_columns.size(); i++) {
		const auto &col = bf_operation->build_columns[i];
		bloom_filter_map[col] = make_shared_ptr<BloomFilter>();
	}
}

string PhysicalCreateBF::GetName() const {
    return "CREATE_BF";
}

string PhysicalCreateBF::ToString(ExplainFormat format) const {
    string result = "CREATE_BF";
    result += " [" + std::to_string(bf_operation->build_columns.size()) + " filters]";
    return result;
}

InsertionOrderPreservingMap<string> PhysicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Operator"] = "PhysicalCreateBF";
	result["Build Table"] = to_string(bf_operation->build_table_idx);
	// there can be multiple probe tables for a single create
	string probe_tables;
	vector<idx_t> seen_probe;
	for (const auto &col : bf_operation->probe_columns) {
		bool found = false;
		for (auto idx : seen_probe) { if (idx == col.table_index) { found = true; break; } }
		if (!found) {
			if (!probe_tables.empty()) probe_tables += ", ";
			probe_tables += to_string(col.table_index);
			seen_probe.push_back(col.table_index);
		}
	}
	result["Probe Tables"] = probe_tables;

	string build_cols = "";
	for (size_t i = 0; i < bf_operation->build_columns.size(); i++) {
		if (i > 0) {
			build_cols += ", ";
		}
		build_cols += "(" + to_string(bf_operation->build_columns[i].table_index) +
					 "." + to_string(bf_operation->build_columns[i].column_index) + ")";
	}
	result["Build Columns"] = build_cols;

	if (estimated_cardinality != DConstants::INVALID_INDEX) {
		result["Estimated Cardinality"] = std::to_string(estimated_cardinality);
	}

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
	if (!profiling_checked) {
		profiling_checked = true;
		auto prof = GetRPTProfilingState(context.client);
		if (prof) {
			profiling_stats = prof->RegisterCreateBF(
			    bf_operation->build_table_idx, bf_operation->probe_columns, bf_operation->sequence_number);
		}
	}

	CreateBFLocalSinkState &local_state = input.local_state.Cast<CreateBFLocalSinkState>();
	if (profiling_stats) {
		ScopedTimer timer(profiling_stats->sink_time_us);
		profiling_stats->rows_materialized.fetch_add(chunk.size(), std::memory_order_relaxed);
		local_state.local_data->Append(chunk);
	}
	else {
		local_state.local_data->Append(chunk);
	}
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

class CreateBFFinalizeTask : public ExecutorTask {
public:
	CreateBFFinalizeTask(shared_ptr<Event> event_p, ClientContext &context, CreateBFGlobalSinkState &sink_p,
		idx_t chunk_idx_from_p, idx_t chunk_idx_to_p, size_t thread_id_p)
			: ExecutorTask(context, event_p, sink_p.op), event(std::move(event_p)), sink(sink_p),
			  chunk_idx_from(chunk_idx_from_p), chunk_idx_to(chunk_idx_to_p),
			  thread_id(thread_id_p) {

	}

	TaskExecutionResult ExecuteTask(TaskExecutionMode mode) override {
		ThreadContext tcontext(this->executor.context);
		tcontext.profiler.StartOperator(&sink.op);

		for (idx_t i = chunk_idx_from; i < chunk_idx_to; i++) {
			DataChunk chunk;
			sink.total_data->InitializeScanChunk(chunk);
			sink.total_data->FetchChunk(i, chunk);
			for (shared_ptr<BloomFilterBuilder> &bf_builder : sink.bf_builders) {
				bf_builder->PushNextBatch(thread_id, chunk);
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
	size_t thread_id;
};

class CreateBFFinalizeEvent : public BasePipelineEvent {
public:
	CreateBFFinalizeEvent(Pipeline &pipeline_p, CreateBFGlobalSinkState &sink)
		: BasePipelineEvent(pipeline_p), sink(sink) {
	}

	CreateBFGlobalSinkState &sink;

public:
	void Schedule() override {
		auto &context = pipeline->GetClientContext();

		vector<shared_ptr<Task>> finalize_tasks;
		unique_ptr<ColumnDataCollection> &buffer = sink.total_data;
		const auto chunk_count = buffer->ChunkCount();

		const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
		if (num_threads == 1 || (buffer->Count() < PARALLEL_CONSTRUCT_THRESHOLD && !context.config.verify_parallelism)) {
			// single threaded finalize
			finalize_tasks.push_back(make_uniq<CreateBFFinalizeTask>(shared_from_this(), context, sink,
				0, chunk_count, /*thread_id=*/0));
		}
		else {
			// parallel finalize
			auto chunks_per_thread = (chunk_count + num_threads - 1) / num_threads;

			idx_t chunk_idx = 0;
			for (idx_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
				idx_t chunk_idx_from = chunk_idx;
				idx_t chunk_idx_to = MinValue<idx_t>(chunk_idx + chunks_per_thread, chunk_count);
				finalize_tasks.push_back(make_uniq<CreateBFFinalizeTask>(shared_from_this(), context, sink,
					chunk_idx_from, chunk_idx_to, /*thread_id=*/thread_idx));
				chunk_idx = chunk_idx_to;
				if (chunk_idx == chunk_count) {
					break;
				}
			}
		}

		SetTasks(std::move(finalize_tasks));
	}

	void FinishEvent() override {
		// clean up builder resources
		for (auto &builder : sink.bf_builders) {
			if (builder) {
				builder->CleanUp();
			}
		}

		// mark all bloom filters as finalized after parallel building completes
		string build_table = sink.op.bf_operation ? "table_" + std::to_string(sink.op.bf_operation->build_table_idx) : "unknown";
		D_PRINTF("[FINALIZE] CREATE_BF (build=%s): %zu bloom filters",
		         build_table.c_str(), sink.op.bloom_filter_map.size());

		for (auto &entry : sink.op.bloom_filter_map) {
			const ColumnBinding &col = entry.first;
			const shared_ptr<BloomFilter> &bf = entry.second;
			if (bf) {
				bf->Fold();
				bf->finalized_ = true;
				D_PRINTF("[FINALIZE] CREATE_BF (build=%s): Bloom filter for column (%llu.%llu) folded and finalized",
				         build_table.c_str(), (unsigned long long)col.table_index, (unsigned long long)col.column_index);
			}
		}
	}

	// static constexpr const idx_t PARALLEL_CONSTRUCT_THRESHOLD = 1048576;
	static constexpr const idx_t PARALLEL_CONSTRUCT_THRESHOLD = 100000;
};

void CreateBFGlobalSinkState::ScheduleFinalize(Pipeline &pipeline, Event &event) {
	auto new_event = make_shared_ptr<CreateBFFinalizeEvent>(pipeline, *this);
	event.InsertEvent(std::move(new_event));
}


SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	// lazy init profiling if Sink was never called (e.g., empty input)
	if (!profiling_checked) {
		profiling_checked = true;
		auto prof = GetRPTProfilingState(context);
		if (prof) {
			profiling_stats = prof->RegisterCreateBF(
			    bf_operation->build_table_idx, bf_operation->probe_columns, bf_operation->sequence_number);
		}
	}

	ThreadContext tcontext(context);
	tcontext.profiler.StartOperator(this);
	auto &gsink = input.global_state.Cast<CreateBFGlobalSinkState>();
	const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();

	// time the finalize phase (merge + BF init + schedule)
	unique_ptr<ScopedTimer> fin_timer;
	if (profiling_stats) {
		fin_timer = make_uniq<ScopedTimer>(profiling_stats->finalize_time_us);
	}

	// 1. merge local data collections
	for (auto &local_data : gsink.local_data_collections) {
		gsink.total_data->Combine(*local_data);
	}

	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
	D_PRINTF("[FINALIZE] CREATE_BF (build=%s): total_data contains %llu rows",
	         build_table.c_str(), (unsigned long long)gsink.total_data->Count());

	gsink.local_data_collections.clear();

	// 2. initialize bloom filters (iterate over map)
	lock_guard<mutex> lock(gsink.bf_lock);
	for (auto &entry : bloom_filter_map) {
		const shared_ptr<BloomFilter> &bf = entry.second;
		if (bf) {
			bf->Initialize(context, estimated_cardinality);
			bf->finalized_ = false;
		}
	}

	// 3. create builders for each bloom filter
	for (size_t i = 0; i < bf_operation->build_columns.size(); i++) {
		const auto &col = bf_operation->build_columns[i];
		auto it = bloom_filter_map.find(col);
		if (it != bloom_filter_map.end()) {
			auto builder = make_shared_ptr<BloomFilterBuilder>();
			vector<idx_t> bound_cols = {bound_column_indices[i]};
			builder->Begin(it->second, bound_cols, num_threads);
			gsink.bf_builders.emplace_back(builder);
		}
	}

	// 4. schedule parallel finalization
	gsink.ScheduleFinalize(pipeline, event);

	tcontext.profiler.EndOperator(nullptr);
	context.GetExecutor().Flush(tcontext);

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalCreateBF::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CreateBFGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCreateBF::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CreateBFLocalSinkState>(context.client, *this);
}

shared_ptr<BloomFilter> PhysicalCreateBF::GetBloomFilter(const ColumnBinding &col) const {
	auto it = bloom_filter_map.find(col);
	if (it != bloom_filter_map.end()) {
		return it->second;
	}
	return nullptr;
}

//===--------------------------------------------------------------------===//
// Source
//===--------------------------------------------------------------------===//

CreateBFGlobalSourceState::CreateBFGlobalSourceState(ClientContext &context, const PhysicalCreateBF &op)
	: context(context) {
	D_ASSERT(op.sink_state);
	auto &gstate = op.sink_state->Cast<CreateBFGlobalSinkState>();
	gstate.total_data->InitializeScan(scan_state);
	partition_id = 0;
}

idx_t CreateBFGlobalSourceState::MaxThreads() {
	return TaskScheduler::GetScheduler(context).NumberOfThreads();
}

unique_ptr<GlobalSourceState> PhysicalCreateBF::GetGlobalSourceState(ClientContext &context) const {
	auto state = make_uniq<CreateBFGlobalSourceState>(context, *this);

	D_ASSERT(sink_state);
	auto &gsink = sink_state->Cast<CreateBFGlobalSinkState>();

	auto chunk_count = gsink.total_data->ChunkCount();
	auto row_count = gsink.total_data->Count();

#ifdef DEBUG
	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
	Printer::Print(StringUtil::Format("[SOURCE] CREATE_BF (build=%s) GetGlobalSourceState: chunk_count=%llu, row_count=%llu",
		build_table.c_str(), (unsigned long long)chunk_count, (unsigned long long)row_count));
#endif

	const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	auto chunks_per_thread = MaxValue<idx_t>((chunk_count + num_threads - 1) / num_threads, 1);
	idx_t chunk_idx = 0;
	for(idx_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
		if (chunk_idx == chunk_count) {
			break;
		}
		auto chunk_idx_from = chunk_idx;
		auto chunk_idx_to = MinValue<idx_t>(chunk_idx_from + chunks_per_thread, chunk_count);
		state->chunks_todo.emplace_back(chunk_idx_from, chunk_idx_to);
#ifdef DEBUG
		Printer::Print(StringUtil::Format("[SOURCE] CREATE_BF (build=%s) Partition %llu: chunks [%llu, %llu)",
			build_table.c_str(), (unsigned long long)thread_idx, (unsigned long long)chunk_idx_from, (unsigned long long)chunk_idx_to));
#endif
		chunk_idx = chunk_idx_to;
	}
	return unique_ptr_cast<CreateBFGlobalSourceState, GlobalSourceState>(std::move(state));
}

unique_ptr<LocalSourceState> PhysicalCreateBF::GetLocalSourceState(
	ExecutionContext &context, GlobalSourceState &gstate) const {
	return make_uniq<CreateBFLocalSourceState>();
}

// TODO: fetch the chunks parallely
SourceResultType PhysicalCreateBF::GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const {

	auto &gstate = sink_state->Cast<CreateBFGlobalSinkState>();
	auto &lstate = input.local_state.Cast<CreateBFLocalSourceState>();
	auto &state = input.global_state.Cast<CreateBFGlobalSourceState>();

#ifdef DEBUG
	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
#endif

	if(lstate.initial) {
		lstate.local_partition_id = state.partition_id.fetch_add(1);
		lstate.initial = false;

#ifdef DEBUG
		Printer::Print(StringUtil::Format("[SOURCE] CREATE_BF (build=%s) GetData initial: partition_id=%llu, chunks_todo.size()=%zu",
			build_table.c_str(), (unsigned long long)lstate.local_partition_id, state.chunks_todo.size()));
#endif

		if (lstate.local_partition_id >= state.chunks_todo.size()) {
			D_PRINTF("[SOURCE] CREATE_BF No more partitions, returning FINISHED");
			return SourceResultType::FINISHED;
		}
		lstate.chunk_from = state.chunks_todo[lstate.local_partition_id].first;
		lstate.chunk_to = state.chunks_todo[lstate.local_partition_id].second;

		// parallel source
		lstate.local_current_chunk_id = lstate.chunk_from;

#ifdef DEBUG
		Printer::Print(StringUtil::Format("[SOURCE] CREATE_BF (build=%s) Assigned range: [%llu, %llu)",
			build_table.c_str(), (unsigned long long)lstate.chunk_from, (unsigned long long)lstate.chunk_to));
#endif
	}

	// sequential source
	// auto chunk_count = gstate.total_data->ChunkCount();
	//
	// if (lstate.local_current_chunk_id >= chunk_count) {
	// 	return SourceResultType::FINISHED;
	// }
	//
	// if (lstate.local_current_chunk_id == 0) {
	// 	lstate.local_current_chunk_id = lstate.chunk_from;
	// }

	// parallel source
	{
		// auto chunk_count = gstate.total_data->ChunkCount();

		if (lstate.local_current_chunk_id >= lstate.chunk_to) {
			return SourceResultType::FINISHED;
		}
	}
	if (profiling_stats) {
		ScopedTimer timer(profiling_stats->source_time_us);
		gstate.total_data->FetchChunk(lstate.local_current_chunk_id++, chunk);
	} else {
		gstate.total_data->FetchChunk(lstate.local_current_chunk_id++, chunk);
	}
	return SourceResultType::HAVE_MORE_OUTPUT;
}

void PhysicalCreateBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();
	sink_state.reset();

#ifdef DEBUG
	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
#endif

	auto &state = meta_pipeline.GetState();

	// make this operator source of the pipeline
	state.SetPipelineSource(current, *this);

	if (this_pipeline == nullptr) {
		D_PRINTF("[PIPELINE] CREATE_BF (build=%s) creating NEW child pipeline for build-side", build_table.c_str());
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		// CreateChildMetaPipeline() automatically registers the child pipeline as a dependency
		child_meta_pipeline.Build(children[0].get());
		D_PRINTF("[PIPELINE] CREATE_BF (build=%s) child pipeline created", build_table.c_str());
	} else {
		D_PRINTF("[PIPELINE] CREATE_BF (build=%s) adding existing child pipeline as dependency", build_table.c_str());
		current.AddDependency(this_pipeline);
	}

}

void PhysicalCreateBF::BuildPipelinesFromRelated(Pipeline &current,
												   MetaPipeline &meta_pipeline) {
	op_state.reset();

	D_ASSERT(children.size() == 1);

#ifdef DEBUG
	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
	char ptr_str[32];
	snprintf(ptr_str, sizeof(ptr_str), "%p", (void*)this);
	Printer::Print(StringUtil::Format("[PIPELINE] CREATE_BF (build=%s, this=%s) BuildPipelinesFromRelated - USE_BF needs this filter", build_table.c_str(), ptr_str));
#endif

	if (this_pipeline == nullptr) {
		D_PRINTF("[PIPELINE] CREATE_BF creating NEW child pipeline from BuildPipelinesFromRelated");
		auto &child_meta_pipeline = meta_pipeline.CreateChildMetaPipeline(current, *this);
		this_pipeline = child_meta_pipeline.GetBasePipeline();
		child_meta_pipeline.Build(children[0].get());
		D_PRINT("[PIPELINE] CREATE_BF child pipeline created and dependency added automatically");
	} else {
		D_PRINT("[PIPELINE] CREATE_BF adding existing pipeline as dependency");
		current.AddDependency(this_pipeline);
	}

#ifdef DEBUG
	this_pipeline->Print();
#endif
}

} // namespace duckdb