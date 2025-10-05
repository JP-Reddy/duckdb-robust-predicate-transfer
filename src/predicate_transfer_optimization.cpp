
#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "include/dag.hpp"
#include "operators/logical_create_bf.hpp"
#include "operators/logical_use_bf.hpp"

using namespace duckdb;


static std::shared_ptr<FilterPlan> MakeSIPFilterPlan(Expression &build_expr, Expression &probe_expr) {
    auto plan = std::make_shared<FilterPlan>();
    plan->build.push_back(build_expr.Copy());
    plan->apply.push_back(probe_expr.Copy());
    return plan;
}


void SIPOptimizerRule(std::unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		SIPOptimizerRule(child);
	}
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();
		// For each equality join condition
		for (auto &cond : join.conditions) {
			if (cond.comparison == ExpressionType::COMPARE_EQUAL &&
				cond.left->type == ExpressionType::BOUND_COLUMN_REF &&
				cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
				// Build side = left, probe side = right
				auto bf_plan = MakeSIPFilterPlan(*cond.left, *cond.right);

				// Insert LogicalCreateBF on build side (left child)
				auto create_bf = std::make_unique<LogicalCreateBF>(
					std::vector<std::shared_ptr<FilterPlan>>{bf_plan});
				create_bf->AddChild(std::move(join.children[0]));
				join.children[0] = std::move(create_bf);

				// Insert LogicalUseBF on probe side (right child)
				auto use_bf = std::make_unique<LogicalUseBF>(bf_plan);
				use_bf->AddChild(std::move(join.children[1]));
				join.children[1] = std::move(use_bf);

				break;
				}
		}
	}
}