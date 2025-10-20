
#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "include/dag.hpp"
#include "operators/logical_create_bf.hpp"
#include "operators/logical_use_bf.hpp"
#include "predicate_transfer_preoptimization.hpp"
#include "predicate_transfer_optimization.hpp"

namespace duckdb {

void RPTPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &op) {
	graph_manager.Build(*plan);
	return plan;
}

} // namespace duckdb