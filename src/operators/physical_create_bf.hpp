#include "duckdb.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "dag.hpp"

using namespace duckdb;

class PhysicalCreateBF : public PhysicalOperator {
public:
	static constexpr auto TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalCreateBF(vector<LogicalType> types, const vector<shared_ptr<FilterPlan>> &filter_plans);

	// Required virtual methods
	virtual ~PhysicalCreateBF() = default;

	string GetName() const override;
	string ToString(ExplainFormat format = ExplainFormat::DEFAULT) const override;
	
	// Operator interface
	OperatorResultType Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
	                          GlobalOperatorState &gstate, OperatorState &state) const override;

public:
	bool is_probing_side;
	std::vector<std::shared_ptr<FilterPlan>> filter_plans;
	idx_t estimated_cardinality;
};