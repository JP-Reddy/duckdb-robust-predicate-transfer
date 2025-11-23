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
#include "../operators/logical_create_bf.hpp"
#include "../operators/logical_use_bf.hpp"
#include <fmt/format.h>

namespace duckdb {
// class LogicalCreateBF;
// class LogicalUseBF;

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

	// step 1: collect all join operators
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

vector<JoinEdge> RPTOptimizerContextState::LargestRoot(vector<JoinEdge> &edges) {
	// step 1: find largest table by cardinality
	idx_t largest_table_idx = 0;
	idx_t max_cardinality = 0;
	for (auto &table_info : table_mgr.table_ops) {
		if (table_info.estimated_cardinality > max_cardinality) {
			max_cardinality = table_info.estimated_cardinality;
			largest_table_idx = table_info.table_idx;
		}
	}

	// print largest table idx
	printf("Largest table: %llu, cardinality: %llu\n", largest_table_idx, max_cardinality);
	DebugPrintGraph(edges);

	// step 2: build MST (maximum) using Prim's algorithm starting from largest table
	unordered_set<idx_t> mst_nodes;
	vector<JoinEdge> mst_edges;

	mst_nodes.insert(largest_table_idx);

	// print table ops size
	printf("Table ops size: %zu\n", table_mgr.table_ops.size());

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

		if (best_edge) {
			printf("Best edge: %llu <-> %llu (weight=%llu)\n", best_edge->table_a, best_edge->table_b, best_edge->weight);
		}
		if (!best_edge) {

			printf("Warning - Disconnected components found. MST incomplete.\n");
			break;
		}

		mst_edges.push_back(*best_edge);
		mst_nodes.insert(best_edge->table_a);
		mst_nodes.insert(best_edge->table_b);
	}

	return mst_edges;
}

TreeNode* RPTOptimizerContextState::BuildRootedTree(vector<JoinEdge> &mst_edges) const {
	// step 1: find largest table (root)
	idx_t root_table_idx = 0;
	idx_t max_cardinality = 0;
	LogicalOperator* root_op = nullptr;

	for (const auto &table_info : table_mgr.table_ops) {
		if (table_info.estimated_cardinality > max_cardinality) {
			max_cardinality = table_info.estimated_cardinality;
			root_table_idx = table_info.table_idx;
			root_op = table_info.table_op;
		}
	}

	printf("BuildRootedTree: root table = %llu (cardinality=%llu)\n", root_table_idx, max_cardinality);

	// step 2: create nodes for all tables
	unordered_map<idx_t, TreeNode*> table_to_node;
	for (const auto &table_info : table_mgr.table_ops) {
		auto* node = new TreeNode(table_info.table_idx, table_info.table_op);
		table_to_node[table_info.table_idx] = node;
	}

	// step 3: build adjacency list from MST edges (undirected)
	unordered_map<idx_t, vector<pair<idx_t, JoinEdge*>>> adjacency;
	for (auto &edge : mst_edges) {
		adjacency[edge.table_a].push_back({edge.table_b, &edge});
		adjacency[edge.table_b].push_back({edge.table_a, &edge});
	}

	// step 4: BFS from root to assign parent-child relationships and levels
	vector<idx_t> queue;
	unordered_set<idx_t> visited;

	queue.push_back(root_table_idx);
	visited.insert(root_table_idx);
	table_to_node[root_table_idx]->level = 0;

	size_t front = 0;
	while (front < queue.size()) {
		idx_t current = queue[front++];
		TreeNode* current_node = table_to_node[current];

		printf("  Visiting table %llu at level %d\n", current, current_node->level);

		// process all neighbors
		for (auto &[neighbor_idx, edge] : adjacency[current]) {
			if (visited.count(neighbor_idx) == 0) {
				// neighbor is a child of current
				TreeNode* child_node = table_to_node[neighbor_idx];
				child_node->parent = current_node;
				child_node->level = current_node->level + 1;
				child_node->edge_to_parent = edge;

				current_node->children.push_back(child_node);

				queue.push_back(neighbor_idx);
				visited.insert(neighbor_idx);

				printf("    Child: table %llu at level %d\n", neighbor_idx, child_node->level);
			}
		}
	}

	return table_to_node[root_table_idx];
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

		if (bf_op.is_create) {
			// CREATE operation
			printf("BF Op %zu: CREATE_BF on table %llu\n", i, bf_op.build_table_idx);
			printf("  Build columns: ");
			for (const auto &col : bf_op.build_columns) {
				printf("(%llu.%llu) ", col.table_index, col.column_index);
			}
			printf("\n");
		} else {
			// USE operation
			printf("BF Op %zu: USE_BF on table %llu (using BF from table %llu)\n",
				   i, bf_op.probe_table_idx, bf_op.build_table_idx);
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
	}
	printf("\n");
}

std::pair<unordered_map<LogicalOperator*, vector<BloomFilterOperation>>,
          unordered_map<LogicalOperator*, vector<BloomFilterOperation>>>
RPTOptimizerContextState::GenerateStageModifications(const vector<JoinEdge> &mst_edges) {

	// step 1: build rooted tree from MST
	TreeNode* root = BuildRootedTree(const_cast<vector<JoinEdge>&>(mst_edges));

	// step 2: collect all nodes organized by level
	unordered_map<int, vector<TreeNode*>> nodes_by_level;
	int max_level = 0;

	// BFS to collect nodes by level
	vector<TreeNode*> queue;
	queue.push_back(root);
	size_t front = 0;

	while (front < queue.size()) {
		TreeNode* node = queue[front++];
		nodes_by_level[node->level].push_back(node);
		max_level = std::max(max_level, node->level);

		for (TreeNode* child : node->children) {
			queue.push_back(child);
		}
	}

	printf("=== TREE LEVELS ===\n");
	for (int level = 0; level <= max_level; level++) {
		printf("Level %d: ", level);
		for (TreeNode* node : nodes_by_level[level]) {
			printf("table_%llu ", node->table_idx);
		}
		printf("\n");
	}
	printf("\n");

	unordered_map<LogicalOperator*, vector<BloomFilterOperation>> forward_bf_ops;
	unordered_map<LogicalOperator*, vector<BloomFilterOperation>> backward_bf_ops;

	// sequence counter to preserve operation order
	idx_t sequence = 0;

	// step 3: forward pass - bottom-up (leaves to root)
	// process levels from highest (leaves) down to 1
	printf("=== FORWARD PASS (leaves → root) ===\n");
	for (int level = max_level; level >= 1; level--) {
		for (TreeNode* child_node : nodes_by_level[level]) {
			TreeNode* parent_node = child_node->parent;
			JoinEdge* edge = child_node->edge_to_parent;

			// determine which columns belong to child and which to parent
			vector<ColumnBinding> child_columns, parent_columns;

			if (edge->table_a == child_node->table_idx) {
				child_columns = edge->join_columns_a;
				parent_columns = edge->join_columns_b;
			} else {
				child_columns = edge->join_columns_b;
				parent_columns = edge->join_columns_a;
			}

			printf("  Level %d: table_%llu (child) CREATE → table_%llu (parent) USE\n",
				   level, child_node->table_idx, parent_node->table_idx);

			// CREATE_BF on child
			BloomFilterOperation create_op;
			create_op.build_table_idx = child_node->table_idx;
			create_op.probe_table_idx = 0; // not used for CREATE operations
			create_op.build_columns = child_columns;
			create_op.is_create = true;
			create_op.sequence_number = sequence++;
			forward_bf_ops[child_node->table_op].push_back(create_op);

			// USE_BF on parent
			BloomFilterOperation use_op;
			use_op.build_table_idx = child_node->table_idx;
			use_op.probe_table_idx = parent_node->table_idx;
			use_op.build_columns = child_columns;
			use_op.probe_columns = parent_columns;
			use_op.is_create = false;
			use_op.sequence_number = sequence++;
			forward_bf_ops[parent_node->table_op].push_back(use_op);
		}
	}
	printf("\n");

	// step 4: backward pass - top-down (root to leaves)
	// process levels from 1 to max_level
	printf("=== BACKWARD PASS (root → leaves) ===\n");
	for (int level = 1; level <= max_level; level++) {
		for (TreeNode* child_node : nodes_by_level[level]) {
			TreeNode* parent_node = child_node->parent;
			JoinEdge* edge = child_node->edge_to_parent;

			// determine which columns belong to parent and which to child
			vector<ColumnBinding> parent_columns, child_columns;

			if (edge->table_a == parent_node->table_idx) {
				parent_columns = edge->join_columns_a;
				child_columns = edge->join_columns_b;
			} else {
				parent_columns = edge->join_columns_b;
				child_columns = edge->join_columns_a;
			}

			printf("  Level %d: table_%llu (parent) CREATE → table_%llu (child) USE\n",
				   level, parent_node->table_idx, child_node->table_idx);

			// CREATE_BF on parent
			BloomFilterOperation create_op;
			create_op.build_table_idx = parent_node->table_idx;
			create_op.probe_table_idx = 0; // not used for CREATE operations
			create_op.build_columns = parent_columns;
			create_op.is_create = true;
			create_op.sequence_number = sequence++;
			backward_bf_ops[parent_node->table_op].push_back(create_op);

			// USE_BF on child
			BloomFilterOperation use_op;
			use_op.build_table_idx = parent_node->table_idx;
			use_op.probe_table_idx = child_node->table_idx;
			use_op.build_columns = parent_columns;
			use_op.probe_columns = child_columns;
			use_op.is_create = false;
			use_op.sequence_number = sequence++;
			backward_bf_ops[child_node->table_op].push_back(use_op);
		}
	}
	printf("\n");

	return {std::move(forward_bf_ops), std::move(backward_bf_ops)};
}

// std::pair<unordered_map<LogicalOperator*, vector<BloomFilterOperation>>,
// 			unordered_map<LogicalOperator*, vector<BloomFilterOperation>>>
// RPTOptimizerContextState::GenerateStageModifications(const vector<JoinEdge> &mst_edges) {
//
// 	unordered_map<LogicalOperator*, vector<BloomFilterOperation>> forward_bf_ops;
// 	unordered_map<LogicalOperator*, vector<BloomFilterOperation>> backward_bf_ops;
//
// 	for (const JoinEdge &mst_edge: mst_edges) {
// 		// for each edge, create a bf operation
// 		// rule: CREATE_BF on a smaller table, USE_BF on a larger table
//
// 		const idx_t left_cardinality = table_mgr.table_lookup[mst_edge.table_a].estimated_cardinality;
// 		const idx_t right_cardinality = table_mgr.table_lookup[mst_edge.table_b].estimated_cardinality;
//
// 		LogicalOperator* smaller_table_op;
// 		LogicalOperator* larger_table_op;
// 		vector<ColumnBinding> smaller_columns, larger_columns;
//
// 		if (left_cardinality <= right_cardinality) {
// 			smaller_table_op = table_mgr.table_lookup[mst_edge.table_a].table_op;
// 			larger_table_op = table_mgr.table_lookup[mst_edge.table_b].table_op;
// 			smaller_columns = mst_edge.join_columns_a;
// 			larger_columns = mst_edge.join_columns_b;
// 		}
// 		else {
// 			smaller_table_op = table_mgr.table_lookup[mst_edge.table_b].table_op;
// 			larger_table_op = table_mgr.table_lookup[mst_edge.table_a].table_op;
// 			smaller_columns = mst_edge.join_columns_b;
// 			larger_columns = mst_edge.join_columns_a;
// 		}
//
// 		// forward pass: smaller → larger
// 		// CREATE_BF on smaller table
// 		BloomFilterOperation forward_create_bf;
// 		forward_create_bf.build_table_idx = table_mgr.GetScalarTableIndex(smaller_table_op);
// 		forward_create_bf.probe_table_idx = table_mgr.GetScalarTableIndex(larger_table_op);
// 		forward_create_bf.build_columns = smaller_columns;
// 		forward_create_bf.probe_columns = larger_columns;
// 		forward_create_bf.is_create = true;
//
// 		// USE_BF on larger table
// 		BloomFilterOperation forward_use_bf;
// 		forward_use_bf.build_table_idx = table_mgr.GetScalarTableIndex(smaller_table_op);
// 		forward_use_bf.probe_table_idx = table_mgr.GetScalarTableIndex(larger_table_op);
// 		forward_use_bf.build_columns = smaller_columns;
// 		forward_use_bf.probe_columns = larger_columns;
// 		forward_use_bf.is_create = false;
//
// 		forward_bf_ops[smaller_table_op].push_back(forward_create_bf);
// 		forward_bf_ops[larger_table_op].push_back(forward_use_bf);
//
//
// 		// backward pass: larger → smaller
// 		// CREATE_BF operation for larger table
// 		BloomFilterOperation backward_create_bf;
// 		backward_create_bf.build_table_idx = table_mgr.GetScalarTableIndex(larger_table_op);
// 		backward_create_bf.probe_table_idx = table_mgr.GetScalarTableIndex(smaller_table_op);
// 		backward_create_bf.build_columns = larger_columns;
// 		backward_create_bf.probe_columns = smaller_columns;
// 		backward_create_bf.is_create = true;
//
// 		// USE_BF operation for smaller table
// 		BloomFilterOperation backward_use_bf;
// 		backward_use_bf.build_table_idx = table_mgr.GetScalarTableIndex(larger_table_op);
// 		backward_use_bf.probe_table_idx = table_mgr.GetScalarTableIndex(smaller_table_op);
// 		backward_use_bf.build_columns = larger_columns;
// 		backward_use_bf.probe_columns = smaller_columns;
// 		backward_use_bf.is_create = false;
//
// 		backward_bf_ops[larger_table_op].push_back(backward_create_bf);
// 		backward_bf_ops[smaller_table_op].push_back(backward_use_bf);
// 	}
// 	return {std::move(forward_bf_ops), std::move(backward_bf_ops)};
// }

unique_ptr<LogicalOperator> RPTOptimizerContextState::BuildStackedBFOperators(unique_ptr<LogicalOperator> base_plan,
																			   const vector<BloomFilterOperation> &bf_ops,
																			   bool reverse_order) {
	if (bf_ops.empty()) {
		return base_plan;
	}

	printf("BuildStackedBF: Processing %zu ops, reverse=%d\n", bf_ops.size(), reverse_order);

	// start with the base plan at the bottom
	unique_ptr<LogicalOperator> current = std::move(base_plan);

	if (reverse_order) {
		// backward pass: normal iteration (ops already in correct order in vector)
		int iter = 0;
		for (const auto &bf_op : bf_ops) {
			unique_ptr<LogicalOperator> new_op;

			printf("  Bwd iter %d: %s on table %llu\n", iter++, bf_op.is_create ? "CREATE" : "USE",
				   bf_op.is_create ? bf_op.build_table_idx : bf_op.probe_table_idx);

			if (bf_op.is_create) {
				new_op = make_uniq<LogicalCreateBF>(bf_op);
			} else {
				new_op = make_uniq<LogicalUseBF>(bf_op);
			}

			new_op->AddChild(std::move(current));
			current = std::move(new_op);
		}
	} else {
		// forward pass: normal order
		int iter = 0;
		for (const auto &bf_op : bf_ops) {
			unique_ptr<LogicalOperator> new_op;

			printf("  Fwd iter %d: %s on table %llu\n", iter++, bf_op.is_create ? "CREATE" : "USE",
				   bf_op.is_create ? bf_op.build_table_idx : bf_op.probe_table_idx);

			if (bf_op.is_create) {
				new_op = make_uniq<LogicalCreateBF>(bf_op);
			} else {
				new_op = make_uniq<LogicalUseBF>(bf_op);
			}

			new_op->AddChild(std::move(current));
			current = std::move(new_op);
		}
	}

	printf("BuildStackedBF: Done, returning operator\n");
	return current;
}

unique_ptr<LogicalOperator> RPTOptimizerContextState::ApplyStageModifications(unique_ptr<LogicalOperator> plan,
																			  const unordered_map<LogicalOperator*, vector<BloomFilterOperation>> &forward_bf_ops,
																			  const unordered_map<LogicalOperator*, vector<BloomFilterOperation>> &backward_bf_ops) {

	// first apply modifications to children recursively
	for (auto &child : plan->children) {
		child = ApplyStageModifications(std::move(child), forward_bf_ops, backward_bf_ops);
	}

	LogicalOperator* original_op = plan.get();

	// add the forward pass bf operators above the base table operator
	auto forward_it = forward_bf_ops.find(original_op);
	if (forward_it != forward_bf_ops.end()) {
		// printf("ApplyStage: Found %zu forward ops for operator %p\n", forward_it->second.size(), original_op);
		plan = BuildStackedBFOperators(std::move(plan), forward_it->second, false);
	}

	// add the backward pass bf operators above the forward pass bf operators
	auto backward_it = backward_bf_ops.find(original_op);
	if (backward_it != backward_bf_ops.end()) {
		// printf("ApplyStage: Found %zu backward ops for operator %p\n", backward_it->second.size(), original_op);
		for (size_t i = 0; i < backward_it->second.size(); i++) {
			const auto &op = backward_it->second[i];
			// printf("  Backward op %zu: %s on table %llu\n", i, op.is_create ? "CREATE" : "USE", op.is_create ? op.build_table_idx : op.probe_table_idx);
		}
		plan = BuildStackedBFOperators(std::move(plan), backward_it->second, true);
	}

	return plan;
}

unique_ptr<LogicalOperator> RPTOptimizerContextState::Optimize(unique_ptr<LogicalOperator> plan) {

	// step 1: extract join operators
	vector<JoinEdge> edges = ExtractOperators(*plan);

	// step 2: create transfer graph using LargestRoot algorithm
	vector<JoinEdge> mst_edges = LargestRoot(edges);

	// step 3: generate forward/backward pass using MST edges
	const auto bf_ops = GenerateStageModifications(mst_edges);
	const unordered_map<LogicalOperator *, vector<BloomFilterOperation>> forward_bf_ops = bf_ops.first;
	const unordered_map<LogicalOperator *, vector<BloomFilterOperation>> backward_bf_ops = bf_ops.second;

	// step 4: insert create_bf/use_bf operators into the plan
	plan = ApplyStageModifications(std::move(plan), forward_bf_ops, backward_bf_ops);

	// combine all bloom filter operations for debug (preserving order)
	vector<BloomFilterOperation> all_bf_operations;
	for (const auto &pair : bf_ops.first) {
		all_bf_operations.insert(all_bf_operations.end(), pair.second.begin(), pair.second.end());
	}
	for (const auto &pair : bf_ops.second) {
		all_bf_operations.insert(all_bf_operations.end(), pair.second.begin(), pair.second.end());
	}

	// sort by sequence number to restore generation order
	std::sort(all_bf_operations.begin(), all_bf_operations.end(),
		[](const BloomFilterOperation &a, const BloomFilterOperation &b) {
			return a.sequence_number < b.sequence_number;
		});

	// debug print with correct ordering
	DebugPrintMST(mst_edges, all_bf_operations);
	return plan;
}


// extension hooks
// void PredicateTransferOptimizer::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
// 	// create optimizer state using proper DuckDB state management
// 	auto optimizer_state = input.context.registered_state->GetOrCreate<PredicateTransferOptimizer>(
// 		"rpt_optimizer_state", input.context);
//
// 	plan = optimizer_state->PreOptimize(std::move(plan));
// }

void RPTOptimizerContextState::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	// retrieve the optimizer state from ClientContext
	const auto optimizer_state = input.context.registered_state->GetOrCreate<RPTOptimizerContextState>("rpt_optimizer_state", input.context);
	// if (!optimizer_state) {
	// 	optimizer_state = input.context.registered_state->GetOrCreate<PredicateTransferOptimizer>(
	// 		"rpt_optimizer_state", input.context);
	// }

	plan = optimizer_state->Optimize(std::move(plan));

	// cleanup
	input.context.registered_state->Remove("rpt_optimizer_state");
}

} // namespace duckdb
