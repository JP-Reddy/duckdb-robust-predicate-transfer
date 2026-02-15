#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "../optimizer/graph_manager.hpp"
#include "bloom_filter.hpp"

namespace duckdb {

struct UseBFStats;
class PhysicalCreateBF;

class PhysicalUseBFState : public CachingOperatorState {
public:
	PhysicalUseBFState() : bloom_filters_initialized(false), tested_hardcoded(false) {}

	vector<shared_ptr<BloomFilter>> bloom_filters;
	bool bloom_filters_initialized;
	bool tested_hardcoded;
};

class PhysicalUseBF : public CachingPhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalUseBF(PhysicalPlan &physical_plan, shared_ptr<BloomFilterOperation> bf_operation, vector<LogicalType> types,
	             idx_t estimated_cardinality, vector<idx_t> bound_column_indices);

	// required virtual methods
	virtual ~PhysicalUseBF() = default;

	string GetName() const override;
	string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const override;

	// populate info in query plan
	InsertionOrderPreservingMap<string> ParamsToString() const override;

	// state management
	unique_ptr<OperatorState> GetOperatorState(ExecutionContext &context) const override;

	bool ParallelOperator() const override {
		return true;
	}

	void BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) override;

protected:
	// operator interface - using ExecuteInternal for CachingPhysicalOperator
	OperatorResultType ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                                   GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	shared_ptr<BloomFilterOperation> bf_operation;

	// maps the column indices to resolved chunk column positions
	vector<idx_t> bound_column_indices;

	// references to related CREATE_BF operators
	vector<PhysicalCreateBF*> related_create_bf_vec;
	mutable PhysicalCreateBF *related_create_bf = nullptr;

	// profiling
	mutable shared_ptr<UseBFStats> profiling_stats;
	mutable bool profiling_checked = false;
};

} // namespace duckdb