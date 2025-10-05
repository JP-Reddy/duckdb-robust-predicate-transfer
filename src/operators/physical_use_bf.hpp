#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "dag.hpp"

namespace duckdb {

class PhysicalUseBF : public PhysicalOperator {
public:
	static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalUseBF(shared_ptr<FilterPlan> filter_plan, vector<LogicalType> types, idx_t estimated_cardinality);

	// Required virtual methods
	virtual ~PhysicalUseBF() = default;

	string GetName() const override;
	string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const override;
	
	// Operator interface
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                          GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	shared_ptr<FilterPlan> filter_plan;
	bool is_probing_side;
};

} // namespace duckdb