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

#include <fmt/format.h>

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
		case LogicalOperatorType::LOGICAL_DELIM_GET:
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

		vector<ColumnBinding> left_columns, right_columns;
		for(const JoinCondition &cond: join.conditions) {
			if(cond.comparison == ExpressionType::COMPARE_EQUAL &&
				cond.left->type == ExpressionType::BOUND_COLUMN_REF &&
				cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
				left_columns.push_back(cond.left->Cast<BoundColumnRefExpression>().binding);
				right_columns.push_back(cond.right->Cast<BoundColumnRefExpression>().binding);
			}
		}

		if(!left_columns.empty() && !right_columns.empty()) {
			// use column bindings to determine table indices instead of looking up join children directly
			idx_t left_table_idx = left_columns[0].table_index;
			idx_t right_table_idx = right_columns[0].table_index;
			
			// verify these table indices exist in our table manager
			if(table_mgr.table_lookup.find(left_table_idx) != table_mgr.table_lookup.end() &&
			   table_mgr.table_lookup.find(right_table_idx) != table_mgr.table_lookup.end()) {
				JoinEdge edge(left_table_idx, right_table_idx, left_columns, right_columns, left_columns.size(), join.join_type);
				edges.push_back(edge);
			}
		}
	}

	return edges;
}

vector<BloomFilterOperation> RPTOptimizerContextState::LargestRoot(vector<JoinEdge> &edges) {
	// step 1: find largest table by cardinality
	idx_t largest_table_idx = 0;
	idx_t max_cardinality = 0;
	for (auto &table_info : table_mgr.table_ops) {
		if (table_info.estimated_cardinality > max_cardinality) {
			max_cardinality = table_info.estimated_cardinality;
			largest_table_idx = table_info.table_idx;
		}
	}

	DebugPrintGraph(edges);

	// step 2: build MST (maximum) using Prim's algorithm starting from largest table
	unordered_set<idx_t> mst_nodes;
	vector<JoinEdge> mst_edges;

	mst_nodes.insert(largest_table_idx);

	while (mst_nodes.size() < table_mgr.table_ops.size() && !edges.empty()) {
		const JoinEdge *best_edge = nullptr;
		idx_t max_weight = 0;
		max_cardinality = 0;
		for (JoinEdge &edge : edges) {
			bool left_in_mst = mst_nodes.count(edge.table_a) > 0;
			bool right_in_mst = mst_nodes.count(edge.table_b) > 0;

			if (left_in_mst != right_in_mst) {
				const idx_t weight = edge.weight;
				idx_t left_cardinality = table_mgr.table_lookup[edge.table_a].estimated_cardinality;
				idx_t right_cardinality = table_mgr.table_lookup[edge.table_b].estimated_cardinality;
				const idx_t cardinality = std::min(left_cardinality, right_cardinality);

				if (weight > max_weight || (weight == max_weight && cardinality > max_cardinality)) {
					max_weight = weight;
					max_cardinality = cardinality;
					best_edge = &edge;
				}
			}
		}

		if (!best_edge) {
			printf("Warning - Disconnected components found. MST incomplete.\n");
			break;
		}

		mst_edges.push_back(*best_edge);
		mst_nodes.insert(best_edge->table_a);
		mst_nodes.insert(best_edge->table_b);
	}

	// step 3: Convert MST edges to BloomFilterOperations
	vector<BloomFilterOperation> bf_operations;

	for (const JoinEdge &mst_edge: mst_edges) {
		// for each edge, create a bf operation
		// rule: CREATE_BF on a smaller table, USE_BF on a larger table

		const idx_t left_cardinality = table_mgr.table_lookup[mst_edge.table_a].estimated_cardinality;
		const idx_t right_cardinality = table_mgr.table_lookup[mst_edge.table_b].estimated_cardinality;

		BloomFilterOperation bf_op;

		if (left_cardinality <= right_cardinality) {
			bf_op.build_table_idx = mst_edge.table_a;
			bf_op.probe_table_idx = mst_edge.table_b;
			bf_op.build_columns = mst_edge.join_columns_a;
			bf_op.probe_columns = mst_edge.join_columns_b;
		}
		else {
			bf_op.build_table_idx = mst_edge.table_b;
			bf_op.probe_table_idx = mst_edge.table_a;
			bf_op.build_columns = mst_edge.join_columns_b;
			bf_op.probe_columns = mst_edge.join_columns_a;
		}
		bf_op.join_type = mst_edge.join_type;

		bf_operations.push_back(bf_op);
	}

	DebugPrintMST(mst_edges, bf_operations);
	return bf_operations;
}

void RPTOptimizerContextState::DebugPrintGraph(const vector<JoinEdge> &edges) const {
	// Debug: Print all tables
	printf("=== TABLE INFORMATION ===\n");
	for (const auto &table_info : table_mgr.table_ops) {
		printf("Table %llu: cardinality=%llu\n", table_info.table_idx, table_info.estimated_cardinality);
	}

	// Find largest table
	idx_t largest_table_idx = 0;
	idx_t max_cardinality = 0;
	for (auto &table_info : table_mgr.table_ops) {
		if (table_info.estimated_cardinality > max_cardinality) {
			max_cardinality = table_info.estimated_cardinality;
			largest_table_idx = table_info.table_idx;
		}
	}
	printf("Largest table: %llu (cardinality=%llu)\n\n", largest_table_idx, max_cardinality);

	// Debug: Print all join edges
	printf("=== ALL JOIN EDGES ===\n");
	for (size_t i = 0; i < edges.size(); i++) {
		const auto &edge = edges[i];
		printf("Edge %zu: %llu <-> %llu (weight=%llu, type=%d)\n",
				i, edge.table_a, edge.table_b, edge.weight, (int)edge.join_type);

		// Print column bindings
		printf("  Columns A: ");
		for (const auto &col : edge.join_columns_a) {
			printf("(%llu.%llu) ", col.table_index, col.column_index);
		}
		printf("\n  Columns B: ");
		for (const auto &col : edge.join_columns_b) {
			printf("(%llu.%llu) ", col.table_index, col.column_index);
		}
		printf("\n");
	}
	printf("\n");
}

void RPTOptimizerContextState::DebugPrintMST(const vector<JoinEdge> &mst_edges, const vector<BloomFilterOperation> &bf_operations) {
	printf("=== MST EDGES ===\n");
	for (size_t i = 0; i < mst_edges.size(); i++) {
		const auto &edge = mst_edges[i];
		printf("MST Edge %zu: %llu <-> %llu (weight=%llu)\n",
			i, edge.table_a, edge.table_b, edge.weight);
	}
	printf("\n");

	printf("=== BLOOM FILTER OPERATIONS ===\n");
	for (size_t i = 0; i < bf_operations.size(); i++) {
		const auto &bf_op = bf_operations[i];
		printf("BF Op %zu: CREATE_BF on table %llu -> USE_BF on table %llu\n",
			i, bf_op.build_table_idx, bf_op.probe_table_idx);

		printf("  Build columns: ");
		for (const auto &col : bf_op.build_columns) {
			printf("(%llu.%llu) ", col.table_index, col.column_index);
		}
		printf("\n  Probe columns: ");
		for (const auto &col : bf_op.probe_columns) {
			printf("(%llu.%llu) ", col.table_index, col.column_index);
		}
		printf("\n");
	}
	printf("\n");
}

} // namespace duckdb
