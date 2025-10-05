#pragma once

#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {

// Function declaration - implementation is in predicate_transfer_optimization.cpp
void SIPOptimizerRule(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &op);

} // namespace duckdb