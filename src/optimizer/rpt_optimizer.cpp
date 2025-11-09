#include "rpt_optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
// #include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/common/types.hpp"
#include "table_manager.hpp"
#include "graph_manager.hpp"
#include "duckdb/common/unordered_set.hpp"
#include <algorithm>
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

vector<JoinEdge> RPTOptimizerContextState::ExtractOperators(LogicalOperator &plan) {
	vector<LogicalOperator*> join_ops;
	vector<TableInfo> table_infos;

	// pass 1: collect the base tables and join operators
	ExtractOperatorsRecursive(plan, join_ops);

	// pass 2: create JoinEdges with table information
	return CreateJoinEdges(join_ops);
}


void RPTOptimizerContextState::ExtractOperatorsRecursive(LogicalOperator &plan, vector<LogicalOperator*> &join_ops) {
//	unordered_set<hash_t> existed_set;
//	auto ComputeConditionHash = [](const JoinCondition &cond) {
//		return cond.left->Hash() + cond.right->Hash();
//	};

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
				if (std::any_of(join.conditions.begin(), join.conditions.end(), [](const JoinCondition &jc) {
							return jc.comparison == ExpressionType::COMPARE_EQUAL &&
								   jc.left->type == ExpressionType::BOUND_COLUMN_REF &&
								   jc.right->type == ExpressionType::BOUND_COLUMN_REF;
						})) {
					// JoinEdge edge(join);
					join_ops.push_back(op);
					break;
				}
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

			ExtractOperatorsRecursive(*child, join_ops);
			return;
		}
		case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
			auto &agg = op->Cast<LogicalAggregate>();
			if (agg.groups.empty() && agg.grouping_sets.size() <= 1) {
				table_mgr.AddTableOperator(op);
				ExtractOperatorsRecursive(*op->children[0], join_ops);
			} else {
				auto old_refs = agg.GetColumnBindings();
				for (size_t i = 0; i < agg.groups.size(); i++) {
					if (agg.groups[i]->type == ExpressionType::BOUND_COLUMN_REF) {
						auto &col_ref = agg.groups[i]->Cast<BoundColumnRefExpression>();
						rename_col_bindings.insert({old_refs[i], col_ref.binding});
					}
				}
				ExtractOperatorsRecursive(*op->children[0], join_ops);
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
			ExtractOperatorsRecursive(*op->children[0], join_ops);
			return;
		}
		case LogicalOperatorType::LOGICAL_UNION:
		case LogicalOperatorType::LOGICAL_EXCEPT:
		case LogicalOperatorType::LOGICAL_INTERSECT: {
					table_mgr.AddTableOperator(op);
					ExtractOperatorsRecursive(*op->children[0], join_ops);
					ExtractOperatorsRecursive(*op->children[1], join_ops);
					return;
		}
		case LogicalOperatorType::LOGICAL_WINDOW: {
					table_mgr.AddTableOperator(op);
					ExtractOperatorsRecursive(*op->children[0], join_ops);
					return;
		}
		case LogicalOperatorType::LOGICAL_DUMMY_SCAN:
		case LogicalOperatorType::LOGICAL_EXPRESSION_GET:
		case LogicalOperatorType::LOGICAL_GET:
		case LogicalOperatorType::LOGICAL_EMPTY_RESULT:
		case LogicalOperatorType::LOGICAL_CHUNK_GET:
				table_mgr.AddTableOperator(op);
				return;
		default:
				for (auto &child : op->children) {
					ExtractOperatorsRecursive(*child, join_ops);
				}
		}
}

vector<JoinEdge> RPTOptimizerContextState::CreateJoinEdges(vector<LogicalOperator*> &join_ops) {

	// deduplicate join conditions
//	unordered_set<hash_t> existed_set;
//	auto ComputeConditionHash = [](const JoinCondition &cond) {
//		return cond.left->Hash() + cond.right->Hash();
//	};

	vector<JoinEdge> edges;
	for (auto &op : join_ops) {
		auto &join = op->Cast<LogicalComparisonJoin>();

		// skip duplicate conditions
		//hash_t hash = ComputeConditionHash(join.conditions[0]);
		//if (!existed_set.insert(hash).second) {
		//	continue;
		//}

		vector<ColumnBinding> left_columns, right_columns;
		for(const JoinCondition &cond: join.conditions) {
			if(cond.comparison == ExpressionType::COMPARE_EQUAL &&
				cond.left->type == ExpressionType::BOUND_COLUMN_REF &&
				cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
				left_columns.push_back(cond.left->Cast<BoundColumnRefExpression>().binding);
				right_columns.push_back(cond.right->Cast<BoundColumnRefExpression>().binding);
			}
		}

		// map the table to the join edge
		TableInfo* left_table = table_mgr.GetTableInfo(op->children[0].get());
		TableInfo* right_table = table_mgr.GetTableInfo(op->children[1].get());

		// TODO: should we check the right_columns too? is it necessary?
		if(!left_columns.empty() && !right_columns.empty()) {
			JoinEdge edge(left_table->table_idx, right_table->table_idx, left_columns, right_columns, left_columns.size(), join.join_type);
			edges.push_back(edge);
		}
	}

	return edges;
}

} // namespace duckdb
