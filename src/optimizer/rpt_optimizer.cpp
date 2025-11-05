#include "rpt_optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
// #include "duckdb/planner/operator/logical_get.hpp"

namespace duckdb {

vector<JoinEdge> RPTOptimizerContextState::ExtractOperators(LogicalOperator &plan) {
	unordered_set<hash_t> existed_set;
	auto ComputeConditionHash = [](const JoinCondition &cond) {
		return cond.left->Hash() + cond.right->Hash();
	};

	vector<JoinEdge> join_edges;
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
					JoinEdge edge(join);
					join_edges.push_back(edge);
				}
				break;
			}
			default:
				break;
		}
	}

	switch (op->type) {
		case LogicalOperatorType::LOGICAL_FILTER: {
			LogicalOperator *child = op->children[0].get();
			if(child->type == LogicalOperatorType::LOGICAL_GET) {
				table_mgr.AddTableOperator(child);
				return;
			}

			ExtractOperators(*child);
			return;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &agg = op->Cast<LogicalAggregate>();
			if (agg.groups.empty() && agg.grouping_sets.size() <= 1) {
				table_mgr.AddTableOperator(op);
				ExtractOperators(*op->children[0]);
			} else {
				auto old_refs = agg.GetColumnBindings();
				for (size_t i = 0; i < agg.groups.size(); i++) {
					if (agg.groups[i]->type == ExpressionType::BOUND_COLUMN_REF) {
						auto &col_ref = agg.groups[i]->Cast<BoundColumnRefExpression>();
						rename_col_bindings.insert({old_refs[i], col_ref.binding});
					}
				}
				ExtractOperators(*op->children[0]);
			}
			return;
}
case LogicalOperatorType::LOGICAL_PROJECTION: {
			auto old_refs = op->GetColumnBindings();
			for (size_t i = 0; i < op->expressions.size(); i++) {
				if (op->expressions[i]->type == ExpressionType::BOUND_COLUMN_REF) {
					auto &col_ref = op->expressions[i]->Cast<BoundColumnRefExpression>();
					rename_col_bindings.insert({old_refs[i], col_ref.binding});
				}
			}
			ExtractOperatorsInternal(*op->children[0], joins);
			return;
}
case LogicalOperatorType::LOGICAL_UNION:
case LogicalOperatorType::LOGICAL_EXCEPT:
case LogicalOperatorType::LOGICAL_INTERSECT: {
			AddTableOperator(op);
			ExtractOperatorsInternal(*op->children[0], joins);
			ExtractOperatorsInternal(*op->children[1], joins);
			return;
}
case LogicalOperatorType::LOGICAL_WINDOW: {
			// TODO: how can we handle the window?
			AddTableOperator(op);
			// ExtractOperatorsInternal(*op->children[0], joins);
			return;
}
case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
case LogicalOperatorType::LOGICAL_GET:
case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
case LogicalOperatorType::LOGICAL_CHUNK_GET:
		AddTableOperator(op);
		return;
default:
		for (auto &child : op->children) {
			ExtractOperatorsInternal(*child, joins);
		}
	}
}




	return join_edges;
}
}
