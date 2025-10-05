#include "physical_use_bf.hpp"

namespace duckdb {
struct FilterPlan;
}
using namespace duckdb;

// TODO: Inherit from CachingPhysicalOperator instead of PhysicalOperator
class PhysicalUseBF : public PhysicalOperator {
public:
	std::shared_ptr<FilterPlan> filter_plan;

	PhysicalUseBF(std::shared_ptr<FilterPlan> filter_plan_p, std::vector<LogicalType> types_p)
	: PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types_p), 1), filter_plan(std::move(filter_plan_p)) {}


	OperatorResultType Execute(ExecutionContext &context, DataChunk &chunk, OperatorState &state, GlobalOperatorState &gstate) override {

		// In the real implementation, this would filter the rows in 'chunk'
		// using the Bloom filter stored in filter_plan.
		// For now, just pass through all rows.
		if (!children.empty()) {
			children[0]->Execute(context, chunk, state, gstate);
		}
		return OperatorResultType::NEED_MORE_INPUT;
	}

	std::unique_ptr<PhysicalOperator> Clone() override {
		return std::make_unique<PhysicalUseBF>(filter_plan, types);
	}
};