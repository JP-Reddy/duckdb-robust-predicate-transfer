#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "bloom_filter.hpp"
#include "../optimizer/graph_manager.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include <duckdb/common/types/column/column_data_scan_states.hpp>

namespace duckdb {

struct CreateBFStats;


class PhysicalCreateBF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalCreateBF(PhysicalPlan &physical_plan, const shared_ptr<BloomFilterOperation> bf_operation, vector<LogicalType> types,
	                 idx_t estimated_cardinality, vector<idx_t> bound_column_indices);

	// Required virtual methods
	virtual ~PhysicalCreateBF() = default;

	string GetName() const override;
	string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const override;

	// populate info in query plan
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// sink interface - PhysicalOperator can act as sink
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const override;


	bool IsSink() const override {
		return true;
	}

	bool ParallelSink() const override {
		return true;
	}
	// source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
							  GlobalSourceState &gstate) const override;
	SourceResultType GetDataInternal(ExecutionContext &context, DataChunk &chunk,
				OperatorSourceInput &input) const override;

	bool IsSource() const override {
		return true;
	}

	bool ParallelSource() const override {
		return true;
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;
	void BuildPipelinesFromRelated(Pipeline &current, MetaPipeline &meta_pipeline);

public:
	// vector<shared_ptr<FilterPlan>> filter_plans;
	shared_ptr<BloomFilterOperation> bf_operation;
	bool is_probing_side;

	// maps the column indices to resolved chunk column positions
	vector<idx_t> bound_column_indices;

	// column-keyed bloom filter map: ColumnBinding -> BloomFilter
	unordered_map<ColumnBinding, shared_ptr<BloomFilter>, ColumnBindingHash, ColumnBindingEqual> bloom_filter_map;

	// pipeline reference
	shared_ptr<Pipeline> this_pipeline;

	// lookup bloom filter by the column it was built on
	shared_ptr<BloomFilter> GetBloomFilter(const ColumnBinding &col) const;

	// profiling
	mutable shared_ptr<CreateBFStats> profiling_stats;
	mutable bool profiling_checked = false;
};

class CreateBFLocalSinkState : public LocalSinkState {
public:
	CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op);

	ClientContext &client_context;
	unique_ptr<ColumnDataCollection> local_data;
};

class CreateBFGlobalSinkState : public GlobalSinkState {
public:
	CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op);
	void ScheduleFinalize(Pipeline &pipeline, Event &event);

	const PhysicalCreateBF &op;
	mutex glock;
	mutex bf_lock;
	vector<shared_ptr<BloomFilterBuilder>> bf_builders;

	// store data for sink phase
	unique_ptr<ColumnDataCollection> total_data;
	vector<unique_ptr<ColumnDataCollection>> local_data_collections;
};

class CreateBFLocalSourceState : public LocalSourceState {
public:
	CreateBFLocalSourceState() {
		local_current_chunk_id = 0;
		initial = true;
	}

public:
	idx_t local_current_chunk_id;
	idx_t local_partition_id;
	idx_t chunk_from;
	idx_t chunk_to;
	bool initial;
};

class CreateBFGlobalSourceState : public GlobalSourceState {
public:
	CreateBFGlobalSourceState(ClientContext &context, const PhysicalCreateBF &op);

	idx_t MaxThreads() override;

	ClientContext &context;
	ColumnDataScanState scan_state;
	vector<pair<idx_t, idx_t>> chunks_todo;
	std::atomic<idx_t> partition_id;
	vector<shared_ptr<BloomFilter>> bloom_filters;
	mutex bf_lock;
};

} // namespace duckdb