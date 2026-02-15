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
#include "debug_utils.hpp"
#include "rpt_profiling.hpp"
#include <chrono>

namespace duckdb {
// class LogicalCreateBF;
// class LogicalUseBF;

vector<JoinEdge> RPTOptimizerContextState::ExtractOperators(LogicalOperator &plan) {
	vector<LogicalOperator*> join_ops;
	vector<TableInfo> table_infos;

	// pass 1: collect the base tables and join operators
	ExtractOperatorsRecursive(plan, join_ops);

	// debug: print summary of registered nodes
	D_PRINT("\n=== REGISTERED NODES SUMMARY ===");
	for (const auto &[table_idx, table_info] : table_mgr.table_lookup) {
		D_PRINTF("  table_idx=%llu (type=%d, cardinality=%llu)",
		         (unsigned long long)table_idx, (int)table_info.table_op->type,
		         (unsigned long long)table_info.estimated_cardinality);
	}
	D_PRINTF("Total registered nodes: %zu", table_mgr.table_lookup.size());
	D_PRINTF("Total join operators found: %zu\n", join_ops.size());

	// pass 2: create JoinEdges with table information
	return CreateJoinEdges(join_ops);
}


void RPTOptimizerContextState::ExtractOperatorsRecursive(LogicalOperator &plan, vector<LogicalOperator*> &join_ops) {
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
			// register FILTER as node (not GET)
			D_PRINTF("[NODE_REG] Registering FILTER (child=GET) for table_idx=%llu",
			         (unsigned long long)table_mgr.GetScalarTableIndex(op));
			table_mgr.AddTableOperator(op);
			return;
			}
			else if (child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
			         child->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			// for IN-clause optimization (MARK join), register FILTER as node
			// the table_index comes from the join's left child
			D_PRINTF("[NODE_REG] Registering FILTER (child=JOIN) for table_idx=%llu",
			         (unsigned long long)table_mgr.GetScalarTableIndex(op));
			table_mgr.AddTableOperator(op);
				// still recurse into the join to collect join edges and other tables
				// ExtractOperatorsRecursive(*child, join_ops);
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
	unordered_set<size_t> visited; // to prevent infinite loops

	// follow the rename chain until we find a base table binding
	while (true) {
		// create hash for cycle detection
		size_t hash = std::hash<idx_t>()(current.table_index) ^ (std::hash<idx_t>()(current.column_index) << 1);
		if (visited.count(hash)) {
			// cycle detected, return current binding
		D_PRINTF("WARNING: Cycle detected in rename_col_bindings for binding (%llu.%llu)",
		         (unsigned long long)current.table_index, (unsigned long long)current.column_index);
			break;
		}
		visited.insert(hash);

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

vector<JoinEdge> RPTOptimizerContextState::CreateJoinEdges(vector<LogicalOperator*> &join_ops) {
	vector<JoinEdge> edges;
	for (auto &op : join_ops) {
		auto &join = op->Cast<LogicalComparisonJoin>();

		vector<ColumnBinding> left_columns, right_columns;
		vector<ColumnBinding> resolved_left_columns, resolved_right_columns;

		for(const JoinCondition &cond: join.conditions) {
			if(cond.comparison == ExpressionType::COMPARE_EQUAL &&
				cond.left->type == ExpressionType::BOUND_COLUMN_REF &&
				cond.right->type == ExpressionType::BOUND_COLUMN_REF) {
				// store original bindings
				ColumnBinding left_binding = cond.left->Cast<BoundColumnRefExpression>().binding;
				ColumnBinding right_binding = cond.right->Cast<BoundColumnRefExpression>().binding;

				left_columns.push_back(left_binding);
				right_columns.push_back(right_binding);

				// resolve bindings through rename chain
				resolved_left_columns.push_back(ResolveColumnBinding(left_binding));
				resolved_right_columns.push_back(ResolveColumnBinding(right_binding));
			}
		}

		if(!left_columns.empty() && !right_columns.empty()) {
			// get table indices from first resolved column
			idx_t left_table_idx = resolved_left_columns[0].table_index;
			idx_t right_table_idx = resolved_right_columns[0].table_index;

			// verify these table indices exist in our table manager
			if(table_mgr.table_lookup.find(left_table_idx) != table_mgr.table_lookup.end() &&
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

TreeNode* RPTOptimizerContextState::BuildRootedTree(vector<JoinEdge> &mst_edges) const {

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
	LogicalOperator* root_op = nullptr;
	bool found_root = false;

	for (const auto &table_info : table_mgr.table_ops) {
		if (table_info.estimated_cardinality > max_cardinality) {
			max_cardinality = table_info.estimated_cardinality;
			root_table_idx = table_info.table_idx;
			root_op = table_info.table_op;
			found_root = true;
		}
	}

	if (!found_root) {
		D_PRINT("ERROR: No valid root table found");
		return nullptr;
	}

	// step 2: create nodes for all tables
	unordered_map<idx_t, TreeNode*> table_to_node;
	for (const auto &table_info : table_mgr.table_ops) {
		auto* node = new TreeNode(table_info.table_idx, table_info.table_op);
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

		// check if current node exists
		if (table_to_node.find(current) == table_to_node.end() || !table_to_node[current]) {
			D_PRINTF("ERROR: Node for table %llu not found in table_to_node", (unsigned long long)current);
			continue;
		}

		TreeNode* current_node = table_to_node[current];

		// process all neighbors
		for (auto &[neighbor_idx, edge] : adjacency[current]) {
			if (visited.count(neighbor_idx) == 0) {
				// verify neighbor node exists
				if (table_to_node.find(neighbor_idx) == table_to_node.end() || !table_to_node[neighbor_idx]) {
					D_PRINTF("ERROR: Child node for table %llu not found", (unsigned long long)neighbor_idx);
					continue;
				}

				// neighbor is a child of current
				TreeNode* child_node = table_to_node[neighbor_idx];
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

void RPTOptimizerContextState::DebugPrintGraph([[maybe_unused]] const vector<JoinEdge> &edges) const {
#ifdef DEBUG
	// Debug: Print all tables
	Printer::Print("=== TABLE INFORMATION ===");
	for (const auto &table_info : table_mgr.table_ops) {
		Printer::PrintF("Table %llu: cardinality=%llu", 
		                (unsigned long long)table_info.table_idx, 
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
	Printer::PrintF("Largest table: %llu (cardinality=%llu)\n", 
	                (unsigned long long)largest_table_idx, (unsigned long long)max_cardinality);

	// Debug: Print all join edges
	Printer::Print("=== ALL JOIN EDGES ===");
	for (size_t i = 0; i < edges.size(); i++) {
		const auto &edge = edges[i];
		Printer::PrintF("Edge %zu: %llu <-> %llu (weight=%llu, type=%d)",
		                i, (unsigned long long)edge.table_a, (unsigned long long)edge.table_b, 
		                (unsigned long long)edge.weight, (int)edge.join_type);

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

void RPTOptimizerContextState::DebugPrintMST([[maybe_unused]] const vector<JoinEdge> &mst_edges, 
                                             [[maybe_unused]] const vector<BloomFilterOperation> &bf_operations) {
#ifdef DEBUG
	Printer::Print("=== MST EDGES ===");
	for (size_t i = 0; i < mst_edges.size(); i++) {
		const auto &edge = mst_edges[i];
		Printer::PrintF("MST Edge %zu: %llu <-> %llu (weight=%llu)",
		                i, (unsigned long long)edge.table_a, (unsigned long long)edge.table_b, 
		                (unsigned long long)edge.weight);
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
			Printer::PrintF("BF Op %zu: USE_BF on table %llu (using BF from table %llu)",
			                i, (unsigned long long)bf_op.probe_table_idx, (unsigned long long)bf_op.build_table_idx);
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

std::pair<unordered_map<LogicalOperator*, vector<BloomFilterOperation>>,
          unordered_map<LogicalOperator*, vector<BloomFilterOperation>>>
RPTOptimizerContextState::GenerateStageModifications(const vector<JoinEdge> &mst_edges) {

	// step 1: build rooted tree from MST
	TreeNode* root = BuildRootedTree(const_cast<vector<JoinEdge>&>(mst_edges));

	// check if tree building failed
	if (!root) {
		D_PRINT("ERROR: BuildRootedTree returned nullptr, returning empty modifications");
		return {{}, {}};
	}

	// step 2: collect all nodes organized by level
	unordered_map<int, vector<TreeNode*>> nodes_by_level;
	int max_level = 0;

	// BFS to collect nodes by level
	vector<TreeNode*> queue;
	queue.push_back(root);
	size_t front = 0;

	while (front < queue.size()) {
		TreeNode* node = queue[front++];
		if (!node) {
			D_PRINT("ERROR: Null node encountered during BFS");
			continue;
		}

		nodes_by_level[node->level].push_back(node);
		max_level = std::max(max_level, node->level);

		for (TreeNode* child : node->children) {
			if (child) {
				queue.push_back(child);
			} else {
				D_PRINT("ERROR: Null child node encountered");
			}
		}
	}

	unordered_map<LogicalOperator*, vector<BloomFilterOperation>> forward_bf_ops;
	unordered_map<LogicalOperator*, vector<BloomFilterOperation>> backward_bf_ops;

	// sequence counter to preserve operation order
	idx_t sequence = 0;

	// sort nodes at each level by cardinality ascending so USE_BFs are generated smallest-first
	for (int level = 1; level <= max_level; level++) {
		std::sort(nodes_by_level[level].begin(), nodes_by_level[level].end(),
		          [](const TreeNode* a, const TreeNode* b) {
			          return a->table_op->estimated_cardinality < b->table_op->estimated_cardinality;
		          });
	}

	// step 3: forward pass - bottom-up (leaves to root)
	// process levels from highest (leaves) down to 1
	for (int level = max_level; level >= 1; level--) {
		for (TreeNode* child_node : nodes_by_level[level]) {
			if (!child_node) {
				D_PRINTF("ERROR: Null child_node at level %d", level);
				continue;
			}

			TreeNode* parent_node = child_node->parent;
			if (!parent_node) {
				D_PRINTF("ERROR: Null parent_node for table %llu at level %d", 
				         (unsigned long long)child_node->table_idx, level);
				continue;
			}

			JoinEdge* edge = child_node->edge_to_parent;
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

	// step 4: backward pass - top-down (root to leaves)
	// process levels from 1 to max_level
	for (int level = 1; level <= max_level; level++) {
		for (TreeNode* child_node : nodes_by_level[level]) {
			if (!child_node) {
				D_PRINTF("ERROR: Null child_node at level %d", level);
				continue;
			}

			TreeNode* parent_node = child_node->parent;
			if (!parent_node) {
				D_PRINTF("ERROR: Null parent_node for table %llu at level %d", 
				         (unsigned long long)child_node->table_idx, level);
				continue;
			}

			JoinEdge* edge = child_node->edge_to_parent;
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


unique_ptr<LogicalOperator> RPTOptimizerContextState::BuildStackedBFOperators(unique_ptr<LogicalOperator> base_plan,
																			   const vector<BloomFilterOperation> &bf_ops,
																			   bool reverse_order) {
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
			while (j < bf_ops.size() &&
			       bf_ops[j].is_create &&
			       bf_ops[j].build_table_idx == bf_op.build_table_idx) {
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
						// __assert(op.build_columns.size() == op.probe_columns.size(),"Merging consecutive CREATE_BFs: Build columns and probe columns size different");
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
		// backward pass
		int iter = 0;
		for (auto it = merged_ops.rbegin(); it != merged_ops.rend(); ++it) {
			const auto &bf_op = *it;
			unique_ptr<LogicalOperator> new_op;

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
		for (const auto &bf_op : merged_ops) {
			unique_ptr<LogicalOperator> new_op;

			if (bf_op.is_create) {
				new_op = make_uniq<LogicalCreateBF>(bf_op);
			} else {
				new_op = make_uniq<LogicalUseBF>(bf_op);
			}

			new_op->AddChild(std::move(current));
			current = std::move(new_op);
		}
	}
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

	// pass 1: collect all CREATE_BF operators (indexed by table only, not columns)
	unordered_map<idx_t, LogicalCreateBF*> create_bf_by_table;
	vector<LogicalOperator*> queue;
	queue.push_back(plan);

	while (!queue.empty()) {
		LogicalOperator *current = queue.back();
		queue.pop_back();

		if (current->type == LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR) {
			auto *create_bf = dynamic_cast<LogicalCreateBF*>(current);
			if (create_bf) {
				idx_t table_idx = create_bf->bf_operation.build_table_idx;
				create_bf_by_table[table_idx] = create_bf;
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
			auto *use_bf = dynamic_cast<LogicalUseBF*>(current);
			if (use_bf) {
				idx_t build_table_idx = use_bf->bf_operation.build_table_idx;

				auto it = create_bf_by_table.find(build_table_idx);
				if (it != create_bf_by_table.end()) {
					use_bf->related_create_bf = it->second;
					it->second->related_use_bf.push_back(use_bf);
				} else {
					D_PRINTF("[LINK] WARNING: No matching CREATE_BF found for USE_BF (probe=table_%llu, build=table_%llu)",
					         (unsigned long long)use_bf->bf_operation.probe_table_idx, (unsigned long long)build_table_idx);
				}
			}
		}

		for (auto &child : current->children) {
			queue.push_back(child.get());
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
	// step 2: create transfer graph using LargestRoot algorithm
	mst_edges = LargestRoot(edges);

	// step 3: generate forward/backward pass using MST edges
	const auto bf_ops = GenerateStageModifications(mst_edges);
	const unordered_map<LogicalOperator *, vector<BloomFilterOperation>> forward_bf_ops = bf_ops.first;
	const unordered_map<LogicalOperator *, vector<BloomFilterOperation>> backward_bf_ops = bf_ops.second;

	// step 4: insert create_bf/use_bf operators into the plan
	plan = ApplyStageModifications(std::move(plan), forward_bf_ops, backward_bf_ops);

	// step 5: link USE_BF operators to their corresponding CREATE_BF operators
	LinkUseBFToCreateBF(plan.get());

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
	auto optimizer_state = input.context.registered_state->GetOrCreate<RPTOptimizerContextState>(
	"rpt_optimizer_state", input.context);

	plan = optimizer_state->PreOptimize(std::move(plan));
}


void RPTOptimizerContextState::Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
	auto profiling = GetRPTProfilingState(input.context);
	auto opt_start = std::chrono::high_resolution_clock::now();

	Printer::Print("\n=== LOGICAL PLAN BEFORE RPT ===");
	Printer::Print(plan->ToString());
	Printer::Print("=== END LOGICAL PLAN ===\n");

	const auto optimizer_state = input.context.registered_state->GetOrCreate<RPTOptimizerContextState>("rpt_optimizer_state", input.context);
	plan = optimizer_state->Optimize(std::move(plan));

	if (profiling) {
		auto opt_end = std::chrono::high_resolution_clock::now();
		profiling->optimizer_time_us =
		    std::chrono::duration_cast<std::chrono::microseconds>(opt_end - opt_start).count();
	}

	input.context.registered_state->Remove("rpt_optimizer_state");
}

} // namespace duckdb
