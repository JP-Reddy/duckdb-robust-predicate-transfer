#include "rpt_optimizer.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/common/types.hpp"
#include "table_manager.hpp"
#include "graph_manager.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/set.hpp"
#include <algorithm>
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "../operators/logical_create_bf.hpp"
#include "../operators/logical_use_bf.hpp"
#include "debug_utils.hpp"
#include "rpt_profiling.hpp"
#include "../utils/dag_printer.hpp"
#include <chrono>

namespace duckdb {
// class LogicalCreateBF;
// class LogicalUseBF;

vector<JoinEdge> RPTOptimizerContextState::ExtractOperators(LogicalOperator &plan) {
	vector<LogicalOperator *> join_ops;
	vector<TableInfo> table_infos;

	// pass 1: collect the base tables and join operators
	ExtractOperatorsRecursive(plan, join_ops);

	// debug: print summary of registered nodes
	D_PRINT("\n=== REGISTERED NODES SUMMARY ===");
	for (const auto &entry : table_mgr.table_lookup) {
		D_PRINTF("  table_idx=%llu (type=%d, cardinality=%llu)", (unsigned long long)entry.first,
		         (int)entry.second.table_op->type, (unsigned long long)entry.second.estimated_cardinality);
	}
	D_PRINTF("Total registered nodes: %zu", table_mgr.table_lookup.size());
	D_PRINTF("Total join operators found: %zu\n", join_ops.size());

	// pass 2: create JoinEdges with table information
	return CreateJoinEdges(join_ops);
}

void RPTOptimizerContextState::ExtractOperatorsRecursive(LogicalOperator &plan, vector<LogicalOperator *> &join_ops) {
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
				    return jc.GetComparisonType() == ExpressionType::COMPARE_EQUAL &&
				           jc.GetLHS().type == ExpressionType::BOUND_COLUMN_REF &&
				           jc.GetRHS().type == ExpressionType::BOUND_COLUMN_REF;
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
		if (child->type == LogicalOperatorType::LOGICAL_GET) {
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
		D_PRINTF("[NODE_REG] Registering base table scan, type=%d", (int)op->type);
		table_mgr.AddTableOperator(op);
		return;
	default:
		for (auto &child : op->children) {
			ExtractOperatorsRecursive(*child, join_ops);
		}
	}
}

ColumnBinding RPTOptimizerContextState::ResolveColumnBinding(const ColumnBinding &binding) const {
	ColumnBinding current = binding;
	set<pair<idx_t, idx_t>> visited;

	// follow the rename chain until we find a base table binding
	while (true) {
		auto key = make_pair(current.table_index, current.column_index);
		if (visited.count(key)) {
			D_PRINTF("WARNING: Cycle detected in rename_col_bindings for binding (%llu.%llu)",
			         (unsigned long long)current.table_index, (unsigned long long)current.column_index);
			break;
		}
		visited.insert(key);

		// check if this binding exists in the rename map
		auto it = rename_col_bindings.find(current);
		if (it != rename_col_bindings.end()) {
			current = it->second;
		} else {
			// no more renames, this is the base binding
			break;
		}
	}

	return current;
}

vector<JoinEdge> RPTOptimizerContextState::CreateJoinEdges(vector<LogicalOperator *> &join_ops) {
	vector<JoinEdge> edges;
	for (auto &op : join_ops) {
		auto &join = op->Cast<LogicalComparisonJoin>();

		vector<ColumnBinding> left_columns, right_columns;
		vector<ColumnBinding> resolved_left_columns, resolved_right_columns;

		for (const JoinCondition &cond : join.conditions) {
			if (cond.GetComparisonType() == ExpressionType::COMPARE_EQUAL &&
			    cond.GetLHS().type == ExpressionType::BOUND_COLUMN_REF &&
			    cond.GetRHS().type == ExpressionType::BOUND_COLUMN_REF) {
				// store original bindings
				ColumnBinding left_binding = cond.GetLHS().Cast<BoundColumnRefExpression>().binding;
				ColumnBinding right_binding = cond.GetRHS().Cast<BoundColumnRefExpression>().binding;

				left_columns.push_back(left_binding);
				right_columns.push_back(right_binding);

				// resolve bindings through rename chain
				resolved_left_columns.push_back(ResolveColumnBinding(left_binding));
				resolved_right_columns.push_back(ResolveColumnBinding(right_binding));
			}
		}

		if (!left_columns.empty() && !right_columns.empty()) {
			// get table indices from first resolved column
			idx_t left_table_idx = resolved_left_columns[0].table_index;
			idx_t right_table_idx = resolved_right_columns[0].table_index;

			// verify these table indices exist in our table manager
			if (table_mgr.table_lookup.find(left_table_idx) != table_mgr.table_lookup.end() &&
			    table_mgr.table_lookup.find(right_table_idx) != table_mgr.table_lookup.end()) {
				// use resolved column bindings in the JoinEdge so they match child bindings in CreatePlan
				JoinEdge edge(left_table_idx, right_table_idx, resolved_left_columns, resolved_right_columns,
				              resolved_left_columns.size(), join.join_type);
				edges.push_back(edge);
			} else {
				D_PRINTF("WARNING: Resolved table indices (%llu, %llu) not found in table_lookup",
				         (unsigned long long)left_table_idx, (unsigned long long)right_table_idx);
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

				// safely lookup cardinalities with bounds checking
				auto left_it = table_mgr.table_lookup.find(edge.table_a);
				auto right_it = table_mgr.table_lookup.find(edge.table_b);

				if (left_it == table_mgr.table_lookup.end() || right_it == table_mgr.table_lookup.end()) {
					// printf("WARNING: Table lookup failed for edge %llu <-> %llu\n", edge.table_a, edge.table_b);
					continue;
				}

				idx_t left_cardinality = left_it->second.estimated_cardinality;
				idx_t right_cardinality = right_it->second.estimated_cardinality;
				const idx_t cardinality = std::min(left_cardinality, right_cardinality);

				if (weight > max_weight || (weight == max_weight && cardinality > max_cardinality)) {
					max_weight = weight;
					max_cardinality = cardinality;
					best_edge = &edge;
				}
			}
		}

		if (!best_edge) {
			D_PRINT("Warning - Disconnected components found. MST incomplete.");
			// TODO: Add Assertion
			break;
		}

		mst_edges.push_back(*best_edge);
		mst_nodes.insert(best_edge->table_a);
		mst_nodes.insert(best_edge->table_b);
	}

	return mst_edges;
}

TreeNode *RPTOptimizerContextState::BuildRootedTree(vector<JoinEdge> &mst_edges) const {
	if (mst_edges.empty()) {
		return nullptr;
	}

	if (table_mgr.table_ops.empty()) {
		D_PRINT("ERROR: BuildRootedTree called with empty table_ops");
		return nullptr;
	}

	// step 1: find largest table (root)
	idx_t root_table_idx = 0;
	idx_t max_cardinality = 0;
	bool found_root = false;

	for (const auto &table_info : table_mgr.table_ops) {
		if (table_info.estimated_cardinality > max_cardinality) {
			max_cardinality = table_info.estimated_cardinality;
			root_table_idx = table_info.table_idx;
			found_root = true;
		}
	}

	if (!found_root) {
		D_PRINT("ERROR: No valid root table found");
		return nullptr;
	}

	// step 2: create nodes for all tables
	unordered_map<idx_t, TreeNode *> table_to_node;
	for (const auto &table_info : table_mgr.table_ops) {
		auto *node = new TreeNode(table_info.table_idx, table_info.table_op);
		table_to_node[table_info.table_idx] = node;
	}

	// verify root node was created
	if (table_to_node.find(root_table_idx) == table_to_node.end() || !table_to_node[root_table_idx]) {
		D_PRINTF("ERROR: Failed to create root node for table %llu", (unsigned long long)root_table_idx);
		// cleanup allocated nodes
		for (auto &pair : table_to_node) {
			delete pair.second;
		}
		return nullptr;
	}

	// step 3: build adjacency list from MST edges (undirected)
	unordered_map<idx_t, vector<pair<idx_t, JoinEdge *>>> adjacency;
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

		// check if current node exists
		if (table_to_node.find(current) == table_to_node.end() || !table_to_node[current]) {
			D_PRINTF("ERROR: Node for table %llu not found in table_to_node", (unsigned long long)current);
			continue;
		}

		TreeNode *current_node = table_to_node[current];

		// process all neighbors
		for (pair<idx_t, JoinEdge *> &adj_entry : adjacency[current]) {
			idx_t &neighbor_idx = adj_entry.first;
			JoinEdge *&edge = adj_entry.second;
			if (visited.count(neighbor_idx) == 0) {
				// verify neighbor node exists
				if (table_to_node.find(neighbor_idx) == table_to_node.end() || !table_to_node[neighbor_idx]) {
					D_PRINTF("ERROR: Child node for table %llu not found", (unsigned long long)neighbor_idx);
					continue;
				}

				// neighbor is a child of current
				TreeNode *child_node = table_to_node[neighbor_idx];
				child_node->parent = current_node;
				child_node->level = current_node->level + 1;
				child_node->edge_to_parent = edge;

				current_node->children.push_back(child_node);

				queue.push_back(neighbor_idx);
				visited.insert(neighbor_idx);
			}
		}
	}

	return table_to_node[root_table_idx];
}

void RPTOptimizerContextState::DebugPrintGraph(const vector<JoinEdge> &edges) const {
	(void)edges;
#ifdef DEBUG
	// Debug: Print all tables
	Printer::Print("=== TABLE INFORMATION ===");
	for (const auto &table_info : table_mgr.table_ops) {
		Printer::PrintF("Table %llu: cardinality=%llu", (unsigned long long)table_info.table_idx,
		                (unsigned long long)table_info.estimated_cardinality);
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
	Printer::PrintF("Largest table: %llu (cardinality=%llu)\n", (unsigned long long)largest_table_idx,
	                (unsigned long long)max_cardinality);

	// Debug: Print all join edges
	Printer::Print("=== ALL JOIN EDGES ===");
	for (size_t i = 0; i < edges.size(); i++) {
		const auto &edge = edges[i];
		Printer::PrintF("Edge %zu: %llu <-> %llu (weight=%llu, type=%d)", i, (unsigned long long)edge.table_a,
		                (unsigned long long)edge.table_b, (unsigned long long)edge.weight, (int)edge.join_type);

		// Print column bindings
		string cols_a = "  Columns A: ";
		for (const auto &col : edge.join_columns_a) {
			cols_a += "(" + std::to_string(col.table_index) + "." + std::to_string(col.column_index) + ") ";
		}
		Printer::Print(cols_a);

		string cols_b = "  Columns B: ";
		for (const auto &col : edge.join_columns_b) {
			cols_b += "(" + std::to_string(col.table_index) + "." + std::to_string(col.column_index) + ") ";
		}
		Printer::Print(cols_b);
	}
	Printer::Print("");
#endif
}

void RPTOptimizerContextState::DebugPrintMST(const vector<JoinEdge> &mst_edges,
                                             const vector<BloomFilterOperation> &bf_operations) {
	(void)mst_edges;
	(void)bf_operations;
#ifdef DEBUG
	Printer::Print("=== MST EDGES ===");
	for (size_t i = 0; i < mst_edges.size(); i++) {
		const auto &edge = mst_edges[i];
		Printer::PrintF("MST Edge %zu: %llu <-> %llu (weight=%llu)", i, (unsigned long long)edge.table_a,
		                (unsigned long long)edge.table_b, (unsigned long long)edge.weight);
	}
	Printer::Print("");

	Printer::Print("=== BLOOM FILTER OPERATIONS ===");
	for (size_t i = 0; i < bf_operations.size(); i++) {
		const auto &bf_op = bf_operations[i];

		if (bf_op.is_create) {
			// CREATE operation
			Printer::PrintF("BF Op %zu: CREATE_BF on table %llu", i, (unsigned long long)bf_op.build_table_idx);
			string cols = "  Build columns: ";
			for (const auto &col : bf_op.build_columns) {
				cols += "(" + std::to_string(col.table_index) + "." + std::to_string(col.column_index) + ") ";
			}
			Printer::Print(cols);
		} else {
			// USE operation
			Printer::PrintF("BF Op %zu: USE_BF on table %llu (using BF from table %llu)", i,
			                (unsigned long long)bf_op.probe_table_idx, (unsigned long long)bf_op.build_table_idx);
			string build_cols = "  Build columns: ";
			for (const auto &col : bf_op.build_columns) {
				build_cols += "(" + std::to_string(col.table_index) + "." + std::to_string(col.column_index) + ") ";
			}
			Printer::Print(build_cols);

			string probe_cols = "  Probe columns: ";
			for (const auto &col : bf_op.probe_columns) {
				probe_cols += "(" + std::to_string(col.table_index) + "." + std::to_string(col.column_index) + ") ";
			}
			Printer::Print(probe_cols);
		}
	}
	Printer::Print("");
#endif
}

void RPTOptimizerContextState::PrintDAG(TreeNode *root) {
	Value val;
	if (!context.TryGetCurrentSetting("rpt_display_dag", val) || !val.GetValue<bool>()) {
		return;
	}
	PrintTransferDAG(root, table_mgr);
}

// helper: collect all base table indices in a subtree
static void CollectBaseTableIndices(LogicalOperator *op, TableManager &table_mgr, unordered_set<idx_t> &indices) {
	if (!op) {
		return;
	}
	auto *info = table_mgr.GetTableInfo(op);
	if (info) {
		indices.insert(info->table_idx);
		return;
	}
	for (auto &child : op->children) {
		CollectBaseTableIndices(child.get(), table_mgr, indices);
	}
}

// union-find helpers for column equivalence classes
using ColKey = std::pair<idx_t, idx_t>; // (table_idx, column_idx)

static ColKey UFFind(map<ColKey, ColKey> &parent, ColKey x) {
	if (parent.find(x) == parent.end()) {
		parent[x] = x;
	}
	while (parent[x] != x) {
		parent[x] = parent[parent[x]];
		x = parent[x];
	}
	return x;
}

static void UFUnion(map<ColKey, ColKey> &parent, ColKey a, ColKey b) {
	a = UFFind(parent, a);
	b = UFFind(parent, b);
	if (a != b) {
		parent[a] = b;
	}
}

// recursive DFS for building physical DAG
// uses build-first traversal so DFS index matches execution order
// (first-executed = lowest index, last-executed = highest index)
static void PhysicalDAGDFS(LogicalOperator *op, TableManager &table_mgr, RPTOptimizerContextState &state,
                           vector<PhysicalDAGNode *> &all_nodes, map<idx_t, PhysicalDAGNode *> &node_map,
                           map<idx_t, int> &dfs_index, map<ColKey, ColKey> &uf_parent) {
	if (!op) {
		return;
	}

	// base case: registered base table
	auto *info = table_mgr.GetTableInfo(op);
	if (info) {
		bool is_leaf = (op->type == LogicalOperatorType::LOGICAL_GET ||
		                op->type == LogicalOperatorType::LOGICAL_DUMMY_SCAN ||
		                op->type == LogicalOperatorType::LOGICAL_EXPRESSION_GET ||
		                op->type == LogicalOperatorType::LOGICAL_DELIM_GET ||
		                op->type == LogicalOperatorType::LOGICAL_EMPTY_RESULT ||
		                op->type == LogicalOperatorType::LOGICAL_CHUNK_GET);
		if (is_leaf) {
			auto *node = new PhysicalDAGNode(info->table_idx, info->table_op);
			all_nodes.push_back(node);
			node_map[info->table_idx] = node;
			dfs_index[info->table_idx] = (int)all_nodes.size() - 1;
			return;
		}
	}

	// join node
	if (op->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
	    op->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
		auto &join = op->Cast<LogicalComparisonJoin>();

		if (join.join_type == JoinType::MARK) {
			PhysicalDAGDFS(op->children[0].get(), table_mgr, state, all_nodes, node_map, dfs_index, uf_parent);
			return;
		}

		// build-first: visit right child (build) first, then left child (probe)
		// this gives execution order: first-executed tables get lowest DFS index
		PhysicalDAGDFS(op->children[1].get(), table_mgr, state, all_nodes, node_map, dfs_index, uf_parent);
		PhysicalDAGDFS(op->children[0].get(), table_mgr, state, all_nodes, node_map, dfs_index, uf_parent);

		// process each join condition
		for (const JoinCondition &cond : join.conditions) {
			if (cond.GetComparisonType() != ExpressionType::COMPARE_EQUAL) {
				continue;
			}
			if (cond.GetLHS().type != ExpressionType::BOUND_COLUMN_REF ||
			    cond.GetRHS().type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}

			ColumnBinding left_resolved =
			    state.ResolveColumnBinding(cond.GetLHS().Cast<BoundColumnRefExpression>().binding);
			ColumnBinding right_resolved =
			    state.ResolveColumnBinding(cond.GetRHS().Cast<BoundColumnRefExpression>().binding);

			// add to equivalence classes
			ColKey left_key = {left_resolved.table_index, left_resolved.column_index};
			ColKey right_key = {right_resolved.table_index, right_resolved.column_index};
			UFUnion(uf_parent, left_key, right_key);

			idx_t table_a = left_resolved.table_index;
			idx_t table_b = right_resolved.table_index;
			if (!node_map.count(table_a) || !node_map.count(table_b)) {
				continue;
			}
			if (table_a == table_b) {
				continue;
			}

			int idx_a = dfs_index.count(table_a) ? dfs_index[table_a] : -1;
			int idx_b = dfs_index.count(table_b) ? dfs_index[table_b] : -1;

			// higher DFS index = later execution = parent (closer to root/top)
			idx_t parent_idx, child_idx;
			ColumnBinding parent_col, child_col;
			if (idx_a > idx_b) {
				parent_idx = table_a;
				parent_col = left_resolved;
				child_idx = table_b;
				child_col = right_resolved;
			} else {
				parent_idx = table_b;
				parent_col = right_resolved;
				child_idx = table_a;
				child_col = left_resolved;
			}

			// equiv resolution on child side: find shallowest equivalent (highest DFS, != parent)
			ColKey child_root = UFFind(uf_parent, {child_col.table_index, child_col.column_index});
			idx_t best_child = child_idx;
			int best_child_dfs = dfs_index.count(child_idx) ? dfs_index[child_idx] : -1;
			ColumnBinding best_child_col = child_col;

			vector<ColKey> all_keys;
			for (auto &entry : uf_parent) {
				all_keys.push_back(entry.first);
			}
			for (auto &key : all_keys) {
				if (UFFind(uf_parent, key) != child_root) {
					continue;
				}
				idx_t candidate = key.first;
				if (candidate == parent_idx) {
					continue;
				}
				if (!node_map.count(candidate)) {
					continue;
				}
				int candidate_dfs = dfs_index.count(candidate) ? dfs_index[candidate] : -1;
				if (candidate_dfs > best_child_dfs) {
					best_child = candidate;
					best_child_dfs = candidate_dfs;
					best_child_col = ColumnBinding(key.first, key.second);
				}
			}

			auto *parent_node = node_map[parent_idx];
			auto *child_node = node_map[best_child];

			// check if already linked
			bool already_linked = false;
			for (auto *p : child_node->parents) {
				if (p == parent_node) {
					already_linked = true;
					break;
				}
			}

			if (!already_linked) {
				child_node->parents.push_back(parent_node);
				parent_node->children.push_back(child_node);

				PhysicalDAGEdge edge;
				edge.parent_table = parent_idx;
				edge.child_table = best_child;
				edge.parent_cols.push_back(parent_col);
				edge.child_cols.push_back(best_child_col);
				child_node->edges_to_parents.push_back(edge);
			} else {
				// multi-column join: append columns to existing edge
				for (auto &edge : child_node->edges_to_parents) {
					if (edge.parent_table == parent_idx) {
						edge.parent_cols.push_back(parent_col);
						edge.child_cols.push_back(best_child_col);
						break;
					}
				}
			}
		}
		return;
	}

	// other operators: recurse into children
	for (auto &child : op->children) {
		PhysicalDAGDFS(child.get(), table_mgr, state, all_nodes, node_map, dfs_index, uf_parent);
	}
}

vector<PhysicalDAGNode *> RPTOptimizerContextState::BuildPhysicalPlanDAG(LogicalOperator *op) {
	vector<PhysicalDAGNode *> all_nodes;
	map<idx_t, PhysicalDAGNode *> node_map;
	map<idx_t, int> dfs_index;
	map<ColKey, ColKey> uf_parent;

	PhysicalDAGDFS(op, table_mgr, *this, all_nodes, node_map, dfs_index, uf_parent);

	// compute levels: roots (no parents) get 0, others get max(parent levels) + 1
	for (auto *node : all_nodes) {
		node->level = node->parents.empty() ? 0 : -1;
	}
	bool changed = true;
	while (changed) {
		changed = false;
		for (auto *node : all_nodes) {
			if (node->parents.empty()) {
				continue;
			}
			int max_parent = -1;
			bool all_set = true;
			for (auto *p : node->parents) {
				if (p->level < 0) {
					all_set = false;
					break;
				}
				max_parent = std::max(max_parent, p->level);
			}
			if (all_set && max_parent >= 0) {
				int new_level = max_parent + 1;
				if (new_level != node->level) {
					node->level = new_level;
					changed = true;
				}
			}
		}
	}

	return all_nodes;
}

void RPTOptimizerContextState::FlipRootsToLeaves(vector<PhysicalDAGNode *> &all_nodes) {
	// step 1: find all roots
	vector<PhysicalDAGNode *> roots;
	for (auto *node : all_nodes) {
		if (node->parents.empty()) {
			roots.push_back(node);
		}
	}
	if (roots.size() <= 1) {
		return;
	}

	// step 2: find anchor root (largest cardinality)
	PhysicalDAGNode *anchor = roots[0];
	for (auto *r : roots) {
		if (r->table_op->estimated_cardinality > anchor->table_op->estimated_cardinality) {
			anchor = r;
		}
	}

	// step 3: reverse edges for non-anchor roots
	for (auto *root : roots) {
		if (root == anchor) {
			continue;
		}

		// reverse each edge to root's children
		// iterate over a copy since we modify the vectors
		auto children_copy = root->children;
		auto edges_copy = root->edges_to_parents; // edges_to_parents is empty for roots, edges are on children
		for (auto *child : children_copy) {
			// find and remove root from child's parents and get the edge
			PhysicalDAGEdge edge;
			bool found = false;
			for (idx_t i = 0; i < child->parents.size(); i++) {
				if (child->parents[i] == root) {
					edge = child->edges_to_parents[i];
					child->parents.erase(child->parents.begin() + i);
					child->edges_to_parents.erase(child->edges_to_parents.begin() + i);
					found = true;
					break;
				}
			}
			if (!found) {
				continue;
			}

			// remove child from root's children
			for (idx_t i = 0; i < root->children.size(); i++) {
				if (root->children[i] == child) {
					root->children.erase(root->children.begin() + i);
					break;
				}
			}

			// reverse: child becomes parent of root
			child->children.push_back(root);
			root->parents.push_back(child);

			// reverse the edge
			PhysicalDAGEdge reversed;
			reversed.parent_table = edge.child_table;
			reversed.child_table = edge.parent_table;
			reversed.parent_cols = edge.child_cols;
			reversed.child_cols = edge.parent_cols;
			root->edges_to_parents.push_back(reversed);
		}
	}

	// step 4: recompute levels
	for (auto *node : all_nodes) {
		node->level = node->parents.empty() ? 0 : -1;
	}
	bool changed = true;
	while (changed) {
		changed = false;
		for (auto *node : all_nodes) {
			if (node->parents.empty()) {
				continue;
			}
			int max_parent = -1;
			bool all_set = true;
			for (auto *p : node->parents) {
				if (p->level < 0) {
					all_set = false;
					break;
				}
				max_parent = std::max(max_parent, p->level);
			}
			if (all_set && max_parent >= 0) {
				int new_level = max_parent + 1;
				if (new_level != node->level) {
					node->level = new_level;
					changed = true;
				}
			}
		}
	}
}

void RPTOptimizerContextState::PrintPhysicalPlanDAG(LogicalOperator *op) {
	Value val;
	if (!context.TryGetCurrentSetting("rpt_display_physical_dag", val) || !val.GetValue<bool>()) {
		return;
	}

	auto all_nodes = BuildPhysicalPlanDAG(op);
	if (all_nodes.empty()) {
		return;
	}
	PrintPhysicalDAG(all_nodes, table_mgr);
}

std::pair<unordered_map<LogicalOperator *, vector<BloomFilterOperation>>,
          unordered_map<LogicalOperator *, vector<BloomFilterOperation>>>
RPTOptimizerContextState::GenerateStageModifications(const vector<JoinEdge> &mst_edges) {
	// step 1: build rooted tree from MST
	TreeNode *root = BuildRootedTree(const_cast<vector<JoinEdge> &>(mst_edges));

	// check if tree building failed
	if (!root) {
		D_PRINT("ERROR: BuildRootedTree returned nullptr, returning empty modifications");
		return {{}, {}};
	}

	// display DAG if setting is enabled
	PrintDAG(root);

	// step 2: collect all nodes organized by level
	unordered_map<int, vector<TreeNode *>> nodes_by_level;
	int max_level = 0;

	// BFS to collect nodes by level
	vector<TreeNode *> queue;
	queue.push_back(root);
	size_t front = 0;

	while (front < queue.size()) {
		TreeNode *node = queue[front++];
		if (!node) {
			D_PRINT("ERROR: Null node encountered during BFS");
			continue;
		}

		nodes_by_level[node->level].push_back(node);
		max_level = std::max(max_level, node->level);

		for (TreeNode *child : node->children) {
			if (child) {
				queue.push_back(child);
			} else {
				D_PRINT("ERROR: Null child node encountered");
			}
		}
	}

	unordered_map<LogicalOperator *, vector<BloomFilterOperation>> forward_bf_ops;
	unordered_map<LogicalOperator *, vector<BloomFilterOperation>> backward_bf_ops;

	// sequence counter to preserve operation order
	idx_t sequence = 0;

	// sort nodes at each level by cardinality ascending so USE_BFs are generated smallest-first
	for (int level = 1; level <= max_level; level++) {
		std::sort(nodes_by_level[level].begin(), nodes_by_level[level].end(), [](const TreeNode *a, const TreeNode *b) {
			return a->table_op->estimated_cardinality < b->table_op->estimated_cardinality;
		});
	}

	// step 3: forward pass - bottom-up (leaves to root)
	// process levels from highest (leaves) down to 1
	for (int level = max_level; level >= 1; level--) {
		for (TreeNode *child_node : nodes_by_level[level]) {
			if (!child_node) {
				D_PRINTF("ERROR: Null child_node at level %d", level);
				continue;
			}

			TreeNode *parent_node = child_node->parent;
			if (!parent_node) {
				D_PRINTF("ERROR: Null parent_node for table %llu at level %d",
				         (unsigned long long)child_node->table_idx, level);
				continue;
			}

			JoinEdge *edge = child_node->edge_to_parent;
			if (!edge) {
				D_PRINTF("ERROR: Null edge_to_parent for table %llu", (unsigned long long)child_node->table_idx);
				continue;
			}

			// determine which columns belong to child and which to parent
			vector<ColumnBinding> child_columns, parent_columns;

			if (edge->table_a == child_node->table_idx) {
				child_columns = edge->join_columns_a;
				parent_columns = edge->join_columns_b;
			} else {
				child_columns = edge->join_columns_b;
				parent_columns = edge->join_columns_a;
			}

			// CREATE_BF on child
			BloomFilterOperation create_op;
			create_op.build_table_idx = child_node->table_idx;
			create_op.probe_table_idx = parent_node->table_idx;
			create_op.build_columns = child_columns;
			create_op.probe_columns = parent_columns;
			create_op.is_create = true;
			create_op.is_forward_pass = true;
			create_op.sequence_number = sequence++;
			forward_bf_ops[child_node->table_op].push_back(create_op);

			// USE_BF on parent
			BloomFilterOperation use_op;
			use_op.build_table_idx = child_node->table_idx;
			use_op.probe_table_idx = parent_node->table_idx;
			use_op.build_columns = child_columns;
			use_op.probe_columns = parent_columns;
			use_op.is_create = false;
			use_op.is_forward_pass = true;
			use_op.sequence_number = sequence++;
			forward_bf_ops[parent_node->table_op].push_back(use_op);
		}
	}

	// step 4: backward pass - top-down (root to leaves)
	// process levels from 1 to max_level
	for (int level = 1; level <= max_level; level++) {
		for (TreeNode *child_node : nodes_by_level[level]) {
			if (!child_node) {
				D_PRINTF("ERROR: Null child_node at level %d", level);
				continue;
			}

			TreeNode *parent_node = child_node->parent;
			if (!parent_node) {
				D_PRINTF("ERROR: Null parent_node for table %llu at level %d",
				         (unsigned long long)child_node->table_idx, level);
				continue;
			}

			JoinEdge *edge = child_node->edge_to_parent;
			if (!edge) {
				D_PRINTF("ERROR: Null edge_to_parent for table %llu", (unsigned long long)child_node->table_idx);
				continue;
			}

			// determine which columns belong to parent and which to child
			vector<ColumnBinding> parent_columns, child_columns;

			if (edge->table_a == parent_node->table_idx) {
				parent_columns = edge->join_columns_a;
				child_columns = edge->join_columns_b;
			} else {
				parent_columns = edge->join_columns_b;
				child_columns = edge->join_columns_a;
			}

			// CREATE_BF on parent
			BloomFilterOperation create_op;
			create_op.build_table_idx = parent_node->table_idx;
			create_op.probe_table_idx = child_node->table_idx;
			create_op.build_columns = parent_columns;
			create_op.probe_columns = child_columns;
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

	return {std::move(forward_bf_ops), std::move(backward_bf_ops)};
}

std::pair<unordered_map<LogicalOperator *, vector<BloomFilterOperation>>,
          unordered_map<LogicalOperator *, vector<BloomFilterOperation>>>
RPTOptimizerContextState::GenerateStageModificationsFromDAG(vector<PhysicalDAGNode *> &all_nodes) {
	unordered_map<LogicalOperator *, vector<BloomFilterOperation>> forward_bf_ops;
	unordered_map<LogicalOperator *, vector<BloomFilterOperation>> backward_bf_ops;

	if (all_nodes.empty()) {
		return {std::move(forward_bf_ops), std::move(backward_bf_ops)};
	}

	// group nodes by level
	unordered_map<int, vector<PhysicalDAGNode *>> nodes_by_level;
	int max_level = 0;
	for (auto *node : all_nodes) {
		nodes_by_level[node->level].push_back(node);
		max_level = std::max(max_level, node->level);
	}

	idx_t sequence = 0;

	// Printer::Print(StringUtil::Format("[DAG-GEN] %zu nodes, max_level=%d", all_nodes.size(), max_level));
	// for (auto *node : all_nodes) {
	// 	Printer::Print(StringUtil::Format("[DAG-GEN]   table_%llu (level=%d, parents=%zu, children=%zu, card=%llu)",
	// 	                                  (unsigned long long)node->table_idx, node->level, node->parents.size(),
	// 	                                  node->children.size(),
	// 	                                  (unsigned long long)node->table_op->estimated_cardinality));
	// 	for (idx_t ei = 0; ei < node->edges_to_parents.size(); ei++) {
	// 		auto &e = node->edges_to_parents[ei];
	// 		for (idx_t ci = 0; ci < e.parent_cols.size(); ci++) {
	// 			Printer::Print(StringUtil::Format(
	// 			    "[DAG-GEN]     edge[%llu] parent=%llu col(%llu.%llu) <- child=%llu col(%llu.%llu)",
	// 			    (unsigned long long)ei, (unsigned long long)e.parent_table,
	// 			    (unsigned long long)e.parent_cols[ci].table_index,
	// 			    (unsigned long long)e.parent_cols[ci].column_index, (unsigned long long)e.child_table,
	// 			    (unsigned long long)e.child_cols[ci].table_index,
	// 			    (unsigned long long)e.child_cols[ci].column_index));
	// 		}
	// 	}
	// }

	// forward pass: bottom-up (leaves to roots), levels max_level down to 1
	for (int level = max_level; level >= 1; level--) {
		for (auto *child_node : nodes_by_level[level]) {
			for (idx_t ei = 0; ei < child_node->edges_to_parents.size(); ei++) {
				auto &edge = child_node->edges_to_parents[ei];
				auto *parent_node = child_node->parents[ei];

				// CREATE_BF on child (build=child, probe=parent)
				BloomFilterOperation create_op;
				create_op.build_table_idx = child_node->table_idx;
				create_op.probe_table_idx = parent_node->table_idx;
				create_op.build_columns = edge.child_cols;
				create_op.probe_columns = edge.parent_cols;
				create_op.is_create = true;
				create_op.is_forward_pass = true;
				create_op.sequence_number = sequence++;
				forward_bf_ops[child_node->table_op].push_back(create_op);

				// USE_BF on parent
				BloomFilterOperation use_op;
				use_op.build_table_idx = child_node->table_idx;
				use_op.probe_table_idx = parent_node->table_idx;
				use_op.build_columns = edge.child_cols;
				use_op.probe_columns = edge.parent_cols;
				use_op.is_create = false;
				use_op.is_forward_pass = true;
				use_op.sequence_number = sequence++;
				forward_bf_ops[parent_node->table_op].push_back(use_op);
			}
		}
	}

	// backward pass: top-down (roots to leaves), levels 1 to max_level
	for (int level = 1; level <= max_level; level++) {
		for (auto *child_node : nodes_by_level[level]) {
			// collect parent edges with their indices, sort by parent cardinality ascending
			vector<idx_t> edge_indices;
			for (idx_t ei = 0; ei < child_node->edges_to_parents.size(); ei++) {
				edge_indices.push_back(ei);
			}
			std::sort(edge_indices.begin(), edge_indices.end(), [&](idx_t a, idx_t b) {
				return child_node->parents[a]->table_op->estimated_cardinality <
				       child_node->parents[b]->table_op->estimated_cardinality;
			});

			for (auto ei : edge_indices) {
				auto &edge = child_node->edges_to_parents[ei];
				auto *parent_node = child_node->parents[ei];

				// CREATE_BF on parent (build=parent, probe=child)
				BloomFilterOperation create_op;
				create_op.build_table_idx = parent_node->table_idx;
				create_op.probe_table_idx = child_node->table_idx;
				create_op.build_columns = edge.parent_cols;
				create_op.probe_columns = edge.child_cols;
				create_op.is_create = true;
				create_op.is_forward_pass = false;
				create_op.sequence_number = sequence++;
				backward_bf_ops[parent_node->table_op].push_back(create_op);

				// USE_BF on child
				BloomFilterOperation use_op;
				use_op.build_table_idx = parent_node->table_idx;
				use_op.probe_table_idx = child_node->table_idx;
				use_op.build_columns = edge.parent_cols;
				use_op.probe_columns = edge.child_cols;
				use_op.is_create = false;
				use_op.is_forward_pass = false;
				use_op.sequence_number = sequence++;
				backward_bf_ops[child_node->table_op].push_back(use_op);
			}
		}
	}

	return {std::move(forward_bf_ops), std::move(backward_bf_ops)};
}

unique_ptr<LogicalOperator>
RPTOptimizerContextState::BuildStackedBFOperators(unique_ptr<LogicalOperator> base_plan,
                                                  const vector<BloomFilterOperation> &bf_ops, bool reverse_order) {
	if (bf_ops.empty()) {
		return base_plan;
	}

	// preserve order and only merge consecutive CREATEs for the same table
	vector<BloomFilterOperation> merged_ops;

	for (size_t i = 0; i < bf_ops.size(); i++) {
		const auto &bf_op = bf_ops[i];

		if (bf_op.is_create) {
			// Check if we can merge with subsequent consecutive CREATEs for same table
			vector<BloomFilterOperation> consecutive_creates;
			consecutive_creates.push_back(bf_op);

			// Look ahead for consecutive CREATEs on the same table
			size_t j = i + 1;
			while (j < bf_ops.size() && bf_ops[j].is_create && bf_ops[j].build_table_idx == bf_op.build_table_idx) {
				consecutive_creates.push_back(bf_ops[j]);
				j++;
			}

			if (consecutive_creates.size() == 1) {
				// single CREATE, no merging needed
				merged_ops.push_back(bf_op);
			} else {
				// multiple consecutive CREATEs for same table - merge them
				BloomFilterOperation merged_op = consecutive_creates[0];
				merged_op.build_columns.clear();

				// collect all build columns
				for (const auto &op : consecutive_creates) {
					// for (const auto &col : op.build_columns) {
					for (idx_t x = 0; x < op.build_columns.size(); x++) {
						// __assert(op.build_columns.size() == op.probe_columns.size(),"Merging consecutive CREATE_BFs:
						// Build columns and probe columns size different");
						merged_op.build_columns.push_back(op.build_columns[x]);
						merged_op.probe_columns.push_back(op.probe_columns[x]);
					}
				}
				merged_ops.push_back(merged_op);
			}

			// skip the operations we just merged
			i = j - 1;
		} else {
			// USE operation - add as is
			merged_ops.push_back(bf_op);
		}
	}

	// build operators from merged list
	unique_ptr<LogicalOperator> current = std::move(base_plan);

	if (reverse_order) {
		for (auto it = merged_ops.rbegin(); it != merged_ops.rend(); ++it) {
			const auto &bf_op = *it;
			unique_ptr<LogicalOperator> new_op;

			if (bf_op.is_create) {
				auto create = make_uniq<LogicalCreateBF>(bf_op);
				create->is_forward_pass = bf_op.is_forward_pass;
				new_op = std::move(create);
			} else {
				new_op = make_uniq<LogicalUseBF>(bf_op);
			}

			new_op->AddChild(std::move(current));
			current = std::move(new_op);
		}
	} else {
		for (const auto &bf_op : merged_ops) {
			unique_ptr<LogicalOperator> new_op;

			if (bf_op.is_create) {
				auto create = make_uniq<LogicalCreateBF>(bf_op);
				create->is_forward_pass = bf_op.is_forward_pass;
				new_op = std::move(create);
			} else {
				new_op = make_uniq<LogicalUseBF>(bf_op);
			}

			new_op->AddChild(std::move(current));
			current = std::move(new_op);
		}
	}
	return current;
}

unique_ptr<LogicalOperator> RPTOptimizerContextState::ApplyStageModifications(
    unique_ptr<LogicalOperator> plan,
    const unordered_map<LogicalOperator *, vector<BloomFilterOperation>> &forward_bf_ops,
    const unordered_map<LogicalOperator *, vector<BloomFilterOperation>> &backward_bf_ops) {
	// first apply modifications to children recursively
	for (auto &child : plan->children) {
		child = ApplyStageModifications(std::move(child), forward_bf_ops, backward_bf_ops);
	}

	LogicalOperator *original_op = plan.get();

	// add the forward pass bf operators above the base table operator
	auto forward_it = forward_bf_ops.find(original_op);
	if (forward_it != forward_bf_ops.end()) {
		plan = BuildStackedBFOperators(std::move(plan), forward_it->second, false);
	}

	// add the backward pass bf operators above the forward pass bf operators
	auto backward_it = backward_bf_ops.find(original_op);
	if (backward_it != backward_bf_ops.end()) {
		// for (size_t i = 0; i < backward_it->second.size(); i++) {
		// 	const auto &op = backward_it->second[i];
		// }
		plan = BuildStackedBFOperators(std::move(plan), backward_it->second, false);
	}

	return plan;
}

void RPTOptimizerContextState::LinkUseBFToCreateBF(LogicalOperator *plan) {
	if (!plan) {
		return;
	}

	// helper struct to uniquely identify a CREATE_BF
	struct CreateBFKey {
		idx_t build_table_idx;
		vector<ColumnBinding> build_columns;

		bool operator==(const CreateBFKey &other) const {
			if (build_table_idx != other.build_table_idx) {
				return false;
			}
			if (build_columns.size() != other.build_columns.size()) {
				return false;
			}
			for (size_t i = 0; i < build_columns.size(); i++) {
				if (build_columns[i].table_index != other.build_columns[i].table_index ||
				    build_columns[i].column_index != other.build_columns[i].column_index) {
					return false;
				}
			}
			return true;
		}
	};

	struct CreateBFKeyHash {
		size_t operator()(const CreateBFKey &key) const {
			size_t hash = std::hash<idx_t>()(key.build_table_idx);
			for (const auto &col : key.build_columns) {
				hash ^= (std::hash<idx_t>()(col.table_index) << 1);
				hash ^= (std::hash<idx_t>()(col.column_index) << 2);
			}
			return hash;
		}
	};

	// pass 1: collect all CREATE_BF operators (multiple per build table possible)
	unordered_map<idx_t, vector<LogicalCreateBF *>> create_bf_by_table;
	vector<LogicalOperator *> queue;
	queue.push_back(plan);

	while (!queue.empty()) {
		LogicalOperator *current = queue.back();
		queue.pop_back();

		if (current->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
			auto *create_bf = dynamic_cast<LogicalCreateBF *>(current);
			if (create_bf) {
				create_bf_by_table[create_bf->bf_operation.build_table_idx].push_back(create_bf);
			}
		}

		for (auto &child : current->children) {
			queue.push_back(child.get());
		}
	}

	// pass 2: link all USE_BF operators to their corresponding CREATE_BF
	queue.clear();
	queue.push_back(plan);

	while (!queue.empty()) {
		LogicalOperator *current = queue.back();
		queue.pop_back();

		if (current->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
			auto *use_bf = dynamic_cast<LogicalUseBF *>(current);
			if (use_bf) {
				idx_t build_table_idx = use_bf->bf_operation.build_table_idx;
				idx_t probe_table_idx = use_bf->bf_operation.probe_table_idx;

				auto it = create_bf_by_table.find(build_table_idx);
				if (it != create_bf_by_table.end()) {
					for (auto *create_bf : it->second) {
						for (const auto &pc : create_bf->bf_operation.probe_columns) {
							if (pc.table_index == probe_table_idx) {
								use_bf->related_create_bf = create_bf;
								create_bf->related_use_bf.push_back(use_bf);
								break;
							}
						}
						if (use_bf->related_create_bf) {
							break;
						}
					}
					if (!use_bf->related_create_bf) {
						D_PRINTF("[LINK] WARNING: No CREATE_BF with matching probe table for USE_BF "
						         "(build=table_%llu, probe=table_%llu)",
						         (unsigned long long)build_table_idx, (unsigned long long)probe_table_idx);
					}
				} else {
					D_PRINTF("[LINK] WARNING: No CREATE_BF found for USE_BF (build=table_%llu, probe=table_%llu)",
					         (unsigned long long)build_table_idx, (unsigned long long)probe_table_idx);
				}
			}
		}

		for (auto &child : current->children) {
			queue.push_back(child.get());
		}
	}
}

void RPTOptimizerContextState::SetupDynamicFilterPushdown(LogicalOperator *plan) {
	if (!plan) {
		return;
	}

	// collect all forward-pass LogicalCreateBF operators
	vector<LogicalCreateBF *> forward_creates;
	vector<LogicalOperator *> queue;
	queue.push_back(plan);

	while (!queue.empty()) {
		LogicalOperator *current = queue.back();
		queue.pop_back();

		if (current->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
			auto *create_bf = dynamic_cast<LogicalCreateBF *>(current);
			if (create_bf) {
				if (create_bf->is_forward_pass) {
					forward_creates.push_back(create_bf);
				}
			}
		}

		for (auto &child : current->children) {
			queue.push_back(child.get());
		}
	}

	D_PRINTF("[PUSHDOWN-SETUP] found %zu forward CREATE_BFs", forward_creates.size());

	// for each forward-pass CREATE_BF, set up pushdown targets
	for (auto *create_bf : forward_creates) {
		D_PRINTF("[PUSHDOWN-SETUP] CREATE_BF build=table_%llu, related_use_bf=%zu",
		         (unsigned long long)create_bf->bf_operation.build_table_idx, create_bf->related_use_bf.size());
		for (auto *use_bf : create_bf->related_use_bf) {
			if (!use_bf->bf_operation.is_forward_pass) {
				D_PRINTF("[PUSHDOWN-SETUP]   skipping USE_BF probe=table_%llu (not forward)",
				         (unsigned long long)use_bf->bf_operation.probe_table_idx);
				continue;
			}

			idx_t probe_table_idx = use_bf->bf_operation.probe_table_idx;
			auto it = table_mgr.table_lookup.find(probe_table_idx);
			if (it == table_mgr.table_lookup.end()) {
				D_PRINTF("[PUSHDOWN-SETUP]   probe table_%llu not in table_lookup",
				         (unsigned long long)probe_table_idx);
				continue;
			}

			LogicalGet *get = TableManager::FindLogicalGet(it->second.table_op);
			if (!get) {
				continue;
			}

			// create or reuse DynamicTableFilterSet on the LogicalGet
			if (!get->dynamic_filters) {
				get->dynamic_filters = make_shared_ptr<DynamicTableFilterSet>();
			}

			// resolve each probe column to a scan column index
			auto &col_ids = get->GetColumnIds();
			for (size_t i = 0; i < use_bf->bf_operation.probe_columns.size(); i++) {
				const auto &probe_col = use_bf->bf_operation.probe_columns[i];

				idx_t scan_col_idx = probe_col.column_index;
				if (scan_col_idx >= col_ids.size()) {
					D_PRINTF("[PUSHDOWN] probe column (%llu.%llu) out of bounds for scan column_ids (size=%zu)",
					         (unsigned long long)probe_col.table_index, (unsigned long long)probe_col.column_index,
					         col_ids.size());
					continue;
				}

				// get column type and name
				LogicalType col_type = LogicalType::BIGINT;
				string col_name = "col_" + std::to_string(probe_col.column_index);
				idx_t primary_idx = col_ids[scan_col_idx].GetPrimaryIndex();
				if (primary_idx < get->returned_types.size()) {
					col_type = get->returned_types[primary_idx];
				}
				if (primary_idx < get->names.size()) {
					col_name = get->names[primary_idx];
				}

				LogicalCreateBF::DynamicFilterTarget target;
				target.dynamic_filters = get->dynamic_filters;
				target.scan_column_index = scan_col_idx;
				target.probe_column = probe_col;
				target.column_type = col_type;
				target.column_name = col_name;
				create_bf->pushdown_targets.push_back(std::move(target));
			}

			// mark USE_BF as passthrough since filters are pushed to scan
			use_bf->is_passthrough = true;

			D_PRINTF("[PUSHDOWN] forward CREATE_BF (build=table_%llu) -> USE_BF (probe=table_%llu) pushed %zu targets",
			         (unsigned long long)create_bf->bf_operation.build_table_idx, (unsigned long long)probe_table_idx,
			         create_bf->pushdown_targets.size());
		}
	}
}

unique_ptr<LogicalOperator> RPTOptimizerContextState::PreOptimize(unique_ptr<LogicalOperator> plan) {
	// step 1: extract join operators
	vector<JoinEdge> edges = ExtractOperators(*plan);

	// step 2: create transfer graph using LargestRoot algorithm
	mst_edges = LargestRoot(edges);

	return plan;
}

unique_ptr<LogicalOperator> RPTOptimizerContextState::Optimize(unique_ptr<LogicalOperator> plan) {
	// step 1: extract join operators
	vector<JoinEdge> edges = ExtractOperators(*plan);

	D_PRINTF("Edges size: %zu", edges.size());
	if (edges.size() <= 1) {
		return plan;
	}

	// display physical plan DAG if enabled (before we modify the plan)
	PrintPhysicalPlanDAG(plan.get());

	// determine heuristic
	Value heuristic_val;
	string heuristic = "largest_root";
	if (context.TryGetCurrentSetting("rpt_heuristic", heuristic_val)) {
		heuristic = heuristic_val.GetValue<string>();
	}

	unordered_map<LogicalOperator *, vector<BloomFilterOperation>> forward_bf_ops, backward_bf_ops;

	if (heuristic == "join_order") {
		// use DuckDB's join order DAG
		auto all_nodes = BuildPhysicalPlanDAG(plan.get());

		// flip non-largest roots to leaves (default: on)
		Value flip_val;
		bool flip_roots = true;
		if (context.TryGetCurrentSetting("rpt_flip_roots", flip_val)) {
			flip_roots = flip_val.GetValue<bool>();
		}
		if (flip_roots) {
			FlipRootsToLeaves(all_nodes);
		}

		// display DAG if setting is enabled
		Value dag_val;
		if (context.TryGetCurrentSetting("rpt_display_dag", dag_val) && dag_val.GetValue<bool>()) {
			PrintPhysicalDAG(all_nodes, table_mgr);
		}

		auto bf_ops = GenerateStageModificationsFromDAG(all_nodes);
		forward_bf_ops = std::move(bf_ops.first);
		backward_bf_ops = std::move(bf_ops.second);
	} else {
		// default: largest_root
		mst_edges = LargestRoot(edges);
		auto bf_ops = GenerateStageModifications(mst_edges);
		forward_bf_ops = std::move(bf_ops.first);
		backward_bf_ops = std::move(bf_ops.second);
	}

	// check pass mode setting
	Value pass_mode_val;
	string pass_mode = "both";
	if (context.TryGetCurrentSetting("rpt_pass_mode", pass_mode_val)) {
		pass_mode = pass_mode_val.GetValue<string>();
	}
	if (pass_mode == "forward_only") {
		backward_bf_ops.clear();
	}

	// step 4: insert create_bf/use_bf operators into the plan
	plan = ApplyStageModifications(std::move(plan), forward_bf_ops, backward_bf_ops);

	// step 5: link USE_BF operators to their corresponding CREATE_BF operators
	LinkUseBFToCreateBF(plan.get());

	// step 6: set up dynamic filter pushdown for forward-pass operators
	SetupDynamicFilterPushdown(plan.get());

	// // combine all bloom filter operations for debug (preserving order)
	// vector<BloomFilterOperation> all_bf_operations;
	// for (const auto &pair : bf_ops.first) {
	// 	all_bf_operations.insert(all_bf_operations.end(), pair.second.begin(), pair.second.end());
	// }
	// for (const auto &pair : bf_ops.second) {
	// 	all_bf_operations.insert(all_bf_operations.end(), pair.second.begin(), pair.second.end());
	// }
	//
	// // sort by sequence number to restore generation order
	// std::sort(all_bf_operations.begin(), all_bf_operations.end(),
	// 	[](const BloomFilterOperation &a, const BloomFilterOperation &b) {
	// 		return a.sequence_number < b.sequence_number;
	// 	});
	//
	// // debug print with correct ordering
	// DebugPrintMST(mst_edges, all_bf_operations);
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

void RPTOptimizerContextState::PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto optimizer_state =
	    input.context.registered_state->GetOrCreate<RPTOptimizerContextState>("rpt_optimizer_state", input.context);

	plan = optimizer_state->PreOptimize(std::move(plan));
}

void RPTOptimizerContextState::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto profiling = GetRPTProfilingState(input.context);
	auto opt_start = std::chrono::high_resolution_clock::now();

	const auto optimizer_state =
	    input.context.registered_state->GetOrCreate<RPTOptimizerContextState>("rpt_optimizer_state", input.context);
	plan = optimizer_state->Optimize(std::move(plan));

	if (profiling) {
		auto opt_end = std::chrono::high_resolution_clock::now();
		profiling->optimizer_time_us =
		    std::chrono::duration_cast<std::chrono::microseconds>(opt_end - opt_start).count();

		// populate table names for profiling output
		for (const auto &ti : optimizer_state->table_mgr.table_ops) {
			profiling->table_names[ti.table_idx] = optimizer_state->table_mgr.GetTableName(ti.table_idx);
		}
	}

	input.context.registered_state->Remove("rpt_optimizer_state");
}

} // namespace duckdb
