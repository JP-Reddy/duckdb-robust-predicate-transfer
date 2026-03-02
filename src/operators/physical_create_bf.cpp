#include "physical_create_bf.hpp"
#include "bloom_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/parallel/pipeline.hpp"
#include "debug_utils.hpp"
#include "rpt_profiling.hpp"
#include <duckdb/parallel/meta_pipeline.hpp>
#include "duckdb/planner/filter/bloom_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/selectivity_optional_filter.hpp"
#include "duckdb/main/config.hpp"

namespace duckdb {

PhysicalCreateBF::PhysicalCreateBF(PhysicalPlan &physical_plan, const shared_ptr<BloomFilterOperation> bf_operation,
                                   vector<LogicalType> types, idx_t estimated_cardinality,
                                   vector<idx_t> bound_column_indices)
    : PhysicalOperator(physical_plan, PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      bf_operation(bf_operation), is_probing_side(false), bound_column_indices(std::move(bound_column_indices)) {
	// create bloom filter for each build column, keyed by ColumnBinding
	for (size_t i = 0; i < bf_operation->build_columns.size(); i++) {
		const auto &col = bf_operation->build_columns[i];
		bloom_filter_map[col] = make_shared_ptr<PTBloomFilter>();
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
		for (auto idx : seen_probe) {
			if (idx == col.table_index) {
				found = true;
				break;
			}
		}
		if (!found) {
			if (!probe_tables.empty())
				probe_tables += ", ";
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
		build_cols += "(" + to_string(bf_operation->build_columns[i].table_index) + "." +
		              to_string(bf_operation->build_columns[i].column_index) + ")";
	}
	result["Build Columns"] = build_cols;

	if (estimated_cardinality != DConstants::INVALID_INDEX) {
		result["Estimated Cardinality"] = std::to_string(estimated_cardinality);
	}

	return result;
}

//===--------------------------------------------------------------------===//
// Min-Max helpers
//===--------------------------------------------------------------------===//

template <typename T>
static void TypedUpdateMinMax(Vector &vec, idx_t count, ColumnMinMax &mm) {
	UnifiedVectorFormat vdata;
	vec.ToUnifiedFormat(count, vdata);
	auto *data = UnifiedVectorFormat::GetData<T>(vdata);

	T local_min {}, local_max {};
	bool has_val = false;

	for (idx_t row = 0; row < count; row++) {
		auto idx = vdata.sel->get_index(row);
		if (!vdata.validity.RowIsValid(idx)) {
			continue;
		}
		const auto &val = data[idx];
		if (!has_val) {
			local_min = val;
			local_max = val;
			has_val = true;
		} else {
			if (val < local_min) {
				local_min = val;
			}
			if (val > local_max) {
				local_max = val;
			}
		}
	}

	if (!has_val) {
		return;
	}

	Value vmin = Value::CreateValue(local_min);
	Value vmax = Value::CreateValue(local_max);
	if (!mm.has_value) {
		mm.min_val = vmin;
		mm.max_val = vmax;
		mm.has_value = true;
	} else {
		if (vmin < mm.min_val) {
			mm.min_val = vmin;
		}
		if (vmax > mm.max_val) {
			mm.max_val = vmax;
		}
	}
}

static void UpdateMinMax(Vector &vec, idx_t count, ColumnMinMax &mm) {
	auto &type = vec.GetType();
	switch (type.id()) {
	case LogicalTypeId::TINYINT:
		TypedUpdateMinMax<int8_t>(vec, count, mm);
		break;
	case LogicalTypeId::SMALLINT:
		TypedUpdateMinMax<int16_t>(vec, count, mm);
		break;
	case LogicalTypeId::INTEGER:
		TypedUpdateMinMax<int32_t>(vec, count, mm);
		break;
	case LogicalTypeId::BIGINT:
		TypedUpdateMinMax<int64_t>(vec, count, mm);
		break;
	case LogicalTypeId::UTINYINT:
		TypedUpdateMinMax<uint8_t>(vec, count, mm);
		break;
	case LogicalTypeId::USMALLINT:
		TypedUpdateMinMax<uint16_t>(vec, count, mm);
		break;
	case LogicalTypeId::UINTEGER:
		TypedUpdateMinMax<uint32_t>(vec, count, mm);
		break;
	case LogicalTypeId::UBIGINT:
		TypedUpdateMinMax<uint64_t>(vec, count, mm);
		break;
	case LogicalTypeId::FLOAT:
		TypedUpdateMinMax<float>(vec, count, mm);
		break;
	case LogicalTypeId::DOUBLE:
		TypedUpdateMinMax<double>(vec, count, mm);
		break;
	case LogicalTypeId::DATE:
		TypedUpdateMinMax<date_t>(vec, count, mm);
		break;
	case LogicalTypeId::TIMESTAMP:
		TypedUpdateMinMax<timestamp_t>(vec, count, mm);
		break;
	case LogicalTypeId::VARCHAR:
		TypedUpdateMinMax<string_t>(vec, count, mm);
		break;
	default:
		break;
	}
}

//===--------------------------------------------------------------------===//
// Sink
//===--------------------------------------------------------------------===//

CreateBFGlobalSinkState::CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op) : op(op) {
	total_data = make_uniq<ColumnDataCollection>(context, op.types);
	// initialize bloom filters upfront so Sink can insert directly
	for (auto &entry : op.bloom_filter_map) {
		if (entry.second) {
			entry.second->Initialize(context, op.estimated_cardinality);
		}
	}
}

CreateBFLocalSinkState::CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op)
    : client_context(context) {
	local_data = make_uniq<ColumnDataCollection>(client_context, op.types);
	// initialize min-max tracking for each build column
	if (op.is_forward_pass) {
		local_min_max.resize(op.bound_column_indices.size());
	}
}

SinkResultType PhysicalCreateBF::Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const {
	if (!profiling_checked) {
		profiling_checked = true;
		auto prof = GetRPTProfilingState(context.client);
		if (prof) {
			profiling_stats = prof->RegisterCreateBF(bf_operation->build_table_idx, bf_operation->probe_columns,
			                                         bf_operation->sequence_number);
		}
	}

	CreateBFLocalSinkState &local_state = input.local_state.Cast<CreateBFLocalSinkState>();
	if (profiling_stats) {
		ScopedTimer timer(profiling_stats->sink_time_us);
		profiling_stats->rows_materialized.fetch_add(chunk.size(), std::memory_order_relaxed);
		local_state.local_data->Append(chunk);
	} else {
		local_state.local_data->Append(chunk);
	}

	// insert into bloom filters
	for (size_t i = 0; i < bf_operation->build_columns.size(); i++) {
		const auto &col = bf_operation->build_columns[i];
		auto it = bloom_filter_map.find(col);
		if (it != bloom_filter_map.end() && it->second) {
			it->second->Insert(chunk, {bound_column_indices[i]});
		}
	}

	// compute min-max using typed pointer access
	if (is_forward_pass && !local_state.local_min_max.empty() && chunk.size() > 0) {
		for (idx_t i = 0; i < bound_column_indices.size() && i < local_state.local_min_max.size(); i++) {
			idx_t col_idx = bound_column_indices[i];
			if (col_idx >= chunk.ColumnCount()) {
				continue;
			}
			auto &vec = chunk.data[col_idx];
			UpdateMinMax(vec, chunk.size(), local_state.local_min_max[i]);
		}
	}

	return SinkResultType::NEED_MORE_INPUT;
}

SinkCombineResultType PhysicalCreateBF::Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const {
	CreateBFGlobalSinkState &gstate = input.global_state.Cast<CreateBFGlobalSinkState>();
	CreateBFLocalSinkState &local_state = input.local_state.Cast<CreateBFLocalSinkState>();
	lock_guard<mutex> lock(gstate.glock);
	gstate.local_data_collections.emplace_back(std::move(local_state.local_data));

	// merge local min-max into global
	if (!local_state.local_min_max.empty()) {
		if (gstate.column_min_max.empty()) {
			gstate.column_min_max.resize(local_state.local_min_max.size());
		}
		for (idx_t i = 0; i < local_state.local_min_max.size(); i++) {
			auto &local_mm = local_state.local_min_max[i];
			if (!local_mm.has_value) {
				continue;
			}
			auto &global_mm = gstate.column_min_max[i];
			if (!global_mm.has_value) {
				global_mm = local_mm;
			} else {
				if (local_mm.min_val < global_mm.min_val) {
					global_mm.min_val = local_mm.min_val;
				}
				if (local_mm.max_val > global_mm.max_val) {
					global_mm.max_val = local_mm.max_val;
				}
			}
		}
	}

	return SinkCombineResultType::FINISHED;
}

//===--------------------------------------------------------------------===//
// Finalize
//===--------------------------------------------------------------------===//

// pushes dynamic filters (BF + min-max) to table scans after BF is fully built
static void PushDynamicFilters(const PhysicalCreateBF &op, const CreateBFGlobalSinkState &gsink,
                               ClientContext &context) {
	if (!op.is_forward_pass || op.pushdown_targets.empty()) {
		return;
	}

	string filter_type = "all";
	Value filter_type_val;
	if (context.TryGetCurrentSetting("rpt_filter_type", filter_type_val)) {
		filter_type = filter_type_val.GetValue<string>();
	}

	bool push_bf = (filter_type == "all" || filter_type == "bf_only");
	bool push_minmax = (filter_type == "all" || filter_type == "minmax_only");

	for (auto &target : op.pushdown_targets) {
		for (size_t i = 0; i < op.bf_operation->build_columns.size(); i++) {
			if (i >= op.bf_operation->probe_columns.size()) {
				break;
			}
			const auto &probe_col = op.bf_operation->probe_columns[i];
			if (probe_col.table_index != target.probe_column.table_index ||
			    probe_col.column_index != target.probe_column.column_index) {
				continue;
			}

			const auto &build_col = op.bf_operation->build_columns[i];

			if (push_bf) {
				auto bf_it = op.bloom_filter_map.find(build_col);
				if (bf_it != op.bloom_filter_map.end() && bf_it->second && !bf_it->second->IsEmpty()) {
					auto bf_filter = make_uniq<BFTableFilter>(bf_it->second->GetNativeFilter(), false,
					                                          target.column_name, target.column_type);
					auto wrapped = make_uniq<SelectivityOptionalFilter>(std::move(bf_filter),
					                                                    SelectivityOptionalFilter::BF_THRESHOLD,
					                                                    SelectivityOptionalFilter::BF_CHECK_N);
					target.dynamic_filters->PushFilter(op, target.scan_column_index, std::move(wrapped));
					D_PRINTF("[PUSHDOWN] pushed BF for col %s to scan col %llu", target.column_name.c_str(),
					         (unsigned long long)target.scan_column_index);
				}
			}

			if (push_minmax && i < gsink.column_min_max.size() && gsink.column_min_max[i].has_value) {
				auto &mm = gsink.column_min_max[i];
				auto min_filter =
				    make_uniq<ConstantFilter>(ExpressionType::COMPARE_GREATERTHANOREQUALTO, mm.min_val);
				target.dynamic_filters->PushFilter(op, target.scan_column_index, std::move(min_filter));

				auto max_filter = make_uniq<ConstantFilter>(ExpressionType::COMPARE_LESSTHANOREQUALTO, mm.max_val);
				target.dynamic_filters->PushFilter(op, target.scan_column_index, std::move(max_filter));

				D_PRINTF("[PUSHDOWN] pushed min-max for col %s [%s, %s]", target.column_name.c_str(),
				         mm.min_val.ToString().c_str(), mm.max_val.ToString().c_str());
			}

			break;
		}
	}
}

SinkFinalizeType PhysicalCreateBF::Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
                                            OperatorSinkFinalizeInput &input) const {
	// lazy init profiling if Sink was never called (e.g., empty input)
	if (!profiling_checked) {
		profiling_checked = true;
		auto prof = GetRPTProfilingState(context);
		if (prof) {
			profiling_stats = prof->RegisterCreateBF(bf_operation->build_table_idx, bf_operation->probe_columns,
			                                         bf_operation->sequence_number);
		}
	}

	auto &gsink = input.global_state.Cast<CreateBFGlobalSinkState>();

	// time the finalize phase
	unique_ptr<ScopedTimer> fin_timer;
	if (profiling_stats) {
		fin_timer = make_uniq<ScopedTimer>(profiling_stats->finalize_time_us);
	}

	// 1. merge local data collections - needed for downstream Source
	for (auto &local_data : gsink.local_data_collections) {
		gsink.total_data->Combine(*local_data);
	}
	gsink.local_data_collections.clear();

	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
	D_PRINTF("[FINALIZE] CREATE_BF (build=%s): total_data contains %llu rows, %zu bloom filters", build_table.c_str(),
	         (unsigned long long)gsink.total_data->Count(), bloom_filter_map.size());

	// 2. mark bloom filters as finalized
	for (auto &entry : bloom_filter_map) {
		if (entry.second) {
			entry.second->finalized_ = true;
		}
	}

	// 3. push dynamic filters to table scans
	PushDynamicFilters(*this, gsink, context);

	return SinkFinalizeType::READY;
}

unique_ptr<GlobalSinkState> PhysicalCreateBF::GetGlobalSinkState(ClientContext &context) const {
	return make_uniq<CreateBFGlobalSinkState>(context, *this);
}

unique_ptr<LocalSinkState> PhysicalCreateBF::GetLocalSinkState(ExecutionContext &context) const {
	return make_uniq<CreateBFLocalSinkState>(context.client, *this);
}

shared_ptr<PTBloomFilter> PhysicalCreateBF::GetBloomFilter(const ColumnBinding &col) const {
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
	Printer::Print(
	    StringUtil::Format("[SOURCE] CREATE_BF (build=%s) GetGlobalSourceState: chunk_count=%llu, row_count=%llu",
	                       build_table.c_str(), (unsigned long long)chunk_count, (unsigned long long)row_count));
#endif

	const idx_t num_threads = TaskScheduler::GetScheduler(context).NumberOfThreads();
	auto chunks_per_thread = MaxValue<idx_t>((chunk_count + num_threads - 1) / num_threads, 1);
	idx_t chunk_idx = 0;
	for (idx_t thread_idx = 0; thread_idx < num_threads; thread_idx++) {
		if (chunk_idx == chunk_count) {
			break;
		}
		auto chunk_idx_from = chunk_idx;
		auto chunk_idx_to = MinValue<idx_t>(chunk_idx_from + chunks_per_thread, chunk_count);
		state->chunks_todo.emplace_back(chunk_idx_from, chunk_idx_to);
#ifdef DEBUG
		Printer::Print(StringUtil::Format("[SOURCE] CREATE_BF (build=%s) Partition %llu: chunks [%llu, %llu)",
		                                  build_table.c_str(), (unsigned long long)thread_idx,
		                                  (unsigned long long)chunk_idx_from, (unsigned long long)chunk_idx_to));
#endif
		chunk_idx = chunk_idx_to;
	}
	return unique_ptr_cast<CreateBFGlobalSourceState, GlobalSourceState>(std::move(state));
}

unique_ptr<LocalSourceState> PhysicalCreateBF::GetLocalSourceState(ExecutionContext &context,
                                                                   GlobalSourceState &gstate) const {
	return make_uniq<CreateBFLocalSourceState>();
}

SourceResultType PhysicalCreateBF::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                   OperatorSourceInput &input) const {
	auto &gstate = sink_state->Cast<CreateBFGlobalSinkState>();
	auto &lstate = input.local_state.Cast<CreateBFLocalSourceState>();
	auto &state = input.global_state.Cast<CreateBFGlobalSourceState>();

#ifdef DEBUG
	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
#endif

	if (lstate.initial) {
		lstate.local_partition_id = state.partition_id.fetch_add(1);
		lstate.initial = false;

#ifdef DEBUG
		Printer::Print(StringUtil::Format(
		    "[SOURCE] CREATE_BF (build=%s) GetData initial: partition_id=%llu, chunks_todo.size()=%zu",
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
		                                  build_table.c_str(), (unsigned long long)lstate.chunk_from,
		                                  (unsigned long long)lstate.chunk_to));
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

void PhysicalCreateBF::BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	D_ASSERT(children.size() == 1);

#ifdef DEBUG
	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
	char ptr_str[32];
	snprintf(ptr_str, sizeof(ptr_str), "%p", (void *)this);
	Printer::Print(StringUtil::Format(
	    "[PIPELINE] CREATE_BF (build=%s, this=%s) BuildPipelinesFromRelated - USE_BF needs this filter",
	    build_table.c_str(), ptr_str));
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
