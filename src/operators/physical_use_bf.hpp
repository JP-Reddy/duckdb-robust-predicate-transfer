#pragma once

#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "dag.hpp"

namespace duckdb {

class PhysicalUseBF : public PhysicalOperator {
public:
	static constexpr auto TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalUseBF(std::shared_ptr<FilterPlan> filter_plan, std::vector<LogicalType> types_p);

	// Required virtual methods
	virtual ~PhysicalUseBF() = default;

	string GetName() const override;
	string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const override;
	
	// Operator interface
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                          GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	std::shared_ptr<FilterPlan> filter_plan;
	bool is_probing_side;
	idx_t estimated_cardinality;
};

} // namespace duckdb