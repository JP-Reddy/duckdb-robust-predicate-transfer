#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "dag.hpp"

namespace duckdb {

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
	
	// Operator interface
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                          GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	vector<shared_ptr<FilterPlan>> filter_plans;
	bool is_probing_side;
};

} // namespace duckdb