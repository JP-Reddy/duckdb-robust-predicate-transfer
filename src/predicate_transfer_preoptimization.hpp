#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {


void RPTPreOptimizerRule(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &op);

} // namespace duckdb