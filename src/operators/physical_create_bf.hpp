#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "dag.hpp"
#include "bloom_filter.hpp"
#include "../optimizer/graph_manager.hpp"
#include "duckdb/common/types/column/column_data_collection.hpp"
#include <duckdb/common/types/column/column_data_scan_states.hpp>
namespace duckdb {


class PhysicalCreateBF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalCreateBF(const shared_ptr<BloomFilterOperation> bf_operation, vector<LogicalType> types,
	                 idx_t estimated_cardinality, vector<idx_t> bound_column_indices);

	// Required virtual methods
	virtual ~PhysicalCreateBF() = default;

	string GetName() const override;
	string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const override;

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

	// source interface
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override;
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
							  GlobalSourceState &gstate) const override;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk,
				OperatorSourceInput &input) const override;

	bool IsSource() const override {
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

	// pipeline reference
	shared_ptr<Pipeline> this_pipeline;

	// access to created bloom filters for PhysicalUseBF operators
	vector<shared_ptr<BloomFilter>> GetBloomFilters() const;
};

class CreateBFLocalSinkState : public LocalSinkState {
public:
	CreateBFLocalSinkState() = default;
	CreateBFLocalSinkState(ClientContext &context, const PhysicalCreateBF &op);

	ClientContext &client_context;
	unique_ptr<ColumnDataCollection> local_data;
};

class CreateBFGlobalSinkState : public GlobalSinkState {
public:
	CreateBFGlobalSinkState() = default;
	CreateBFGlobalSinkState(ClientContext &context, const PhysicalCreateBF &op);
	void scheduleFinalize(Pipeline &pipeline, Event &event);

	mutex bf_lock;
	const PhysicalCreateBF &op;
	vector<shared_ptr<BloomFilterBuilder>> bf_builders;


	// store data for sink phase
	unique_ptr<ColumnDataCollection> total_data;
	vector<unique_ptr<ColumnDataCollection>> local_data_collections;
};

class PhysicalCreateBFLocalSourceState : public LocalSourceState {
public:
	PhysicalCreateBFLocalSourceState() = default;
};

class PhysicalCreateBFGlobalSourceState : public GlobalSourceState {
public:
	PhysicalCreateBFGlobalSourceState() = default;
	vector<shared_ptr<BloomFilter>> bloom_filters;
	mutex bf_lock;
	ColumnDataScanState scan_state;
};

} // namespace duckdb