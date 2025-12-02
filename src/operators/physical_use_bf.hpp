#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "../optimizer/graph_manager.hpp"
#include "bloom_filter.hpp"

namespace duckdb {
class PhysicalCreateBF;

class PhysicalUseBFState : public OperatorState {
public:
	PhysicalUseBFState() : bloom_filters_initialized(false) {}

	vector<shared_ptr<BloomFilter>> bloom_filters;
	bool bloom_filters_initialized;
};

class PhysicalUseBF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalUseBF(shared_ptr<BloomFilterOperation> bf_operation, vector<LogicalType> types,
	             idx_t estimated_cardinality, vector<idx_t> bound_column_indices);

	// required virtual methods
	virtual ~PhysicalUseBF() = default;

	string GetName() const override;
	string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const override;

	// state management
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	// operator interface
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                          GlobalOperatorState &gstate, OperatorState &state) const override;

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

public:
	shared_ptr<BloomFilterOperation> bf_operation;

	// maps the column indices to resolved chunk column positions
	vector<idx_t> bound_column_indices;

	// references to related CREATE_BF operators
	vector<PhysicalCreateBF*> related_create_bf_vec;
	mutable PhysicalCreateBF *related_create_bf = nullptr;
};

} // namespace duckdb