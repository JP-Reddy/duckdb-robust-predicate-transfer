#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "dag.hpp"
#include "bloom_filter.hpp"

namespace duckdb {

class PhysicalCreateBFLocalSinkState : public LocalSinkState {
public:
	PhysicalCreateBFLocalSinkState() = default;
};

class PhysicalCreateBFGlobalSinkState : public GlobalSinkState {
public:
	PhysicalCreateBFGlobalSinkState() = default;

	vector<shared_ptr<BloomFilter>> bloom_filters;
	mutex bf_lock;
};

class PhysicalCreateBF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalCreateBF(const vector<shared_ptr<FilterPlan>> &filter_plans, vector<LogicalType> types,
	                 idx_t estimated_cardinality);

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
public:
	vector<shared_ptr<FilterPlan>> filter_plans;
	bool is_probing_side;

	// access to created bloom filters for PhysicalUseBF operators
	vector<shared_ptr<BloomFilter>> GetBloomFilters() const;
};

} // namespace duckdb