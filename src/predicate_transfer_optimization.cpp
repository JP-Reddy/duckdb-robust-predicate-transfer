
#include "duckdb.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"

#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "include/dag.hpp"
#include "operators/logical_create_bf.hpp"
#include "operators/logical_use_bf.hpp"

namespace duckdb {

static shared_ptr<FilterPlan> MakeSIPFilterPlan(Expression &build_expr, Expression &probe_expr) {
    auto plan = make_shared_ptr<FilterPlan>();
    plan->build.push_back(build_expr.Copy());
    plan->apply.push_back(probe_expr.Copy());
    
    // Extract bound column indices from the expressions
    if (build_expr.type == ExpressionType::BOUND_COLUMN_REF) {
        auto &build_col_ref = build_expr.Cast<BoundColumnRefExpression>();
        plan->bound_cols_build.push_back(build_col_ref.binding.column_index);
    }
    
    if (probe_expr.type == ExpressionType::BOUND_COLUMN_REF) {
        auto &probe_col_ref = probe_expr.Cast<BoundColumnRefExpression>();
        plan->bound_cols_apply.push_back(probe_col_ref.binding.column_index);
    }
    
    return plan;
}

void SIPOptimizerRule(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &op) {
	for (auto &child : op->children) {
		SIPOptimizerRule(input, child);
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
				auto create_bf = make_uniq<LogicalCreateBF>(
					vector<shared_ptr<FilterPlan>>{bf_plan});
				create_bf->AddChild(std::move(join.children[0]));
				
				// Insert LogicalUseBF on probe side (right child)
				auto use_bf = make_uniq<LogicalUseBF>(bf_plan);
				use_bf->AddChild(std::move(join.children[1]));
				
				// Establish relationship between create and use operators
				use_bf->related_create_bf = create_bf.get();
				
				join.children[0] = std::move(create_bf);
				join.children[1] = std::move(use_bf);

				break;
				}
		}
	}
}

} // namespace duckdb