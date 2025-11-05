#include "rpt_optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
// #include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

vector<JoinEdge> RPTOptimizerContextState::ExtractJoins(LogicalOperator &plan) {
	vector<JoinEdge> joins;
	LogicalOperator *op = &plan;

	// step 1: collect all join edges
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		LogicalComparisonJoin &join = op->Cast<LogicalComparisonJoin>();
		switch (join.join_type) {
			case JoinType::INNER:
			case JoinType::LEFT:
			case JoinType::RIGHT:
			case JoinType::SEMI:
			case JoinType::RIGHT_SEMI: {
				if(std::any_of(join.conditions.begin(), join.conditions.end(), [](const JoinCondition &jc) {
					return jc.comparison == ExpressionType::COMPARE_EQUAL &&
							jc.left->type == ExpressionType::BOUND_COLUMN_REF &&
							jc.right->type == ExpressionType::BOUND_COLUMN_REF;
				})) {
					JoinEdge edge;
					joins.push_back(edge);
				}
				break;
			}
			default:
				break;
		}
	}

	return joins;
}
}
