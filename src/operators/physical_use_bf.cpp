#include "physical_use_bf.hpp"
#include "dag.hpp"

namespace duckdb {

PhysicalUseBF::PhysicalUseBF(shared_ptr<FilterPlan> filter_plan, vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      filter_plan(std::move(filter_plan)), is_probing_side(false) {
}

string PhysicalUseBF::GetName() const {
    return "USE_BF";
}

string PhysicalUseBF::ToString(ExplainFormat format) const {
    return "USE_BF";
}

OperatorResultType PhysicalUseBF::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                         GlobalOperatorState &gstate, OperatorState &state) const {
    // In the real implementation, this would filter the rows in 'chunk'
    // using the Bloom filter stored in filter_plan.
    // For now, just pass through all rows.
    if (!children.empty()) {
        return children[0].get().Execute(context, input, chunk, gstate, state);
    }
    
    chunk.Reference(input);
    return OperatorResultType::HAVE_MORE_OUTPUT;
}

} // namespace duckdb