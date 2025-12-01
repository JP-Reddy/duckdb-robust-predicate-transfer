#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "dag.hpp"
#include "bloom_filter.hpp"

namespace duckdb {
class PhysicalCreateBF;
}

namespace duckdb {

class PhysicalUseBFState : public OperatorState {
public:
	PhysicalUseBFState() : bloom_filters_initialized(false) {}
	
	vector<shared_ptr<BloomFilter>> bloom_filters;
	bool bloom_filters_initialized;
};

class PhysicalUseBFGlobalState : public GlobalOperatorState {
public:
	PhysicalUseBFGlobalState() {}
};

class PhysicalUseBF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalUseBF(shared_ptr<FilterPlan> filter_plan, vector<LogicalType> types, idx_t estimated_cardinality);

	// Required virtual methods
	virtual ~PhysicalUseBF() = default;

	string GetName() const override;
	string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const override;
	
	// State management
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;
	unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &context) const override;
	
	// Operator interface
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                          GlobalOperatorState &gstate, OperatorState &state) const override;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

public:

	shared_ptr<FilterPlan> filter_plan;
	bool is_probing_side;

	vector<PhysicalCreateBF*> related_create_bf_vec;

	// Reference to the corresponding PhysicalCreateBF operator
	mutable PhysicalCreateBF *related_create_bf = nullptr;
	
private:
	void InitializeBloomFilters(PhysicalUseBFState &bf_state, ExecutionContext &context) const;
	bool FilterDataChunk(DataChunk &chunk, const vector<shared_ptr<BloomFilter>> &bloom_filters, 
	                     ExecutionContext &context) const;
};

} // namespace duckdb