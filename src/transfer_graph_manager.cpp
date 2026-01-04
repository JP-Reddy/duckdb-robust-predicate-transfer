#include "transfer_graph_manager.hpp"

#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "predicate_transfer_optimization.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "debug_utils.hpp"

#include <queue>

namespace duckdb {
static ColumnBinding FindBindingRoot(const ColumnBinding &binding, BindingParentMap &parents) {
	auto it = parents.find(binding);
	if (it == parents.end()) {
		return binding;
	}
	ColumnBinding root = it->second;
	if (root != binding) {
		root = FindBindingRoot(root, parents);
		parents[binding] = root; // Path compression
	}
	return root;
}

static void UnionBindings(const ColumnBinding &a, const ColumnBinding &b, const LogicalType &type,
                          BindingParentMap &parents, BindingGroupMap &group_map) {
	ColumnBinding root_a = FindBindingRoot(a, parents);
	ColumnBinding root_b = FindBindingRoot(b, parents);
	if (root_a == root_b) {
		return;
	}

	// Union by attaching b to a
	parents[root_b] = root_a;

	auto &group_a = group_map[root_a];
	if (!group_a) {
		group_a = make_shared_ptr<JoinKeyTableGroup>(type, a.table_index);
	}

	auto &group_b = group_map[root_b];
	if (!group_b) {
		group_b = make_shared_ptr<JoinKeyTableGroup>(type, b.table_index);
	}

	group_a->Union(*group_b);
	group_map[root_a] = group_a;
}

bool TransferGraphManager::Build(LogicalOperator &plan) {
	D_PRINT("\n=== BUILD TRANSFER GRAPH MANAGER ===");
	
	// 1. Extract all operators, including table operators and join operators
	const vector<reference<LogicalOperator>> joins = table_operator_manager.ExtractOperators(plan);
	D_PRINTF("1. extracted operators: %zu table operators, %zu join operators", 
	         table_operator_manager.table_operators.size(), joins.size());
	
	if (table_operator_manager.table_operators.size() < 2) {
		D_PRINT("not enough table operators (< 2), skipping");
		return false;
	}

	// print table operators
	D_PRINT("table operators:");
	for (auto &pair : table_operator_manager.table_operators) {
		auto &op = *pair.second;
		D_PRINTF("  table_idx=%llu, type=%s, cardinality=%llu",
		         (unsigned long long)pair.first, LogicalOperatorToString(op.type).c_str(), 
		         (unsigned long long)op.estimated_cardinality);
	}

	// 2. Getting graph edges information from join operators
	ExtractEdgesInfo(joins);
	D_PRINTF("2. extracted edges: neighbor_matrix size=%zu", neighbor_matrix.size());
	
	if (neighbor_matrix.empty()) {
		D_PRINT("no edges extracted, skipping");
		return false;
	}

	// print edge information
	D_PRINT("edge information:");
	for (auto &pair : neighbor_matrix) {
		idx_t table1 = pair.first;
		for (auto &edge_pair : pair.second) {
			idx_t table2 = edge_pair.first;
			auto &edge = edge_pair.second;
			D_PRINTF("  edge: table_%llu <-> table_%llu, protect_left=%s, protect_right=%s",
			         (unsigned long long)table1, (unsigned long long)table2, 
			         edge->protect_left ? "true" : "false", edge->protect_right ? "true" : "false");
		}
	}

	// 3. unfiltered table only receives bloom filters, they will not generate bloom filters.
	// SkipUnfilteredTable(joins);
	D_PRINTF("3. after SkipUnfilteredTable: neighbor_matrix size=%zu", neighbor_matrix.size());

	// 4. create the transfer graph
	D_PRINT("4. calling CreateTransferPlanUpdated()");
	CreateTransferPlanUpdated();

	D_PRINT("=== BUILD COMPLETE ===\n");
	return true;
}

void TransferGraphManager::AddFilterPlan(idx_t create_table, const shared_ptr<FilterPlan> &filter_plan, bool reverse) {
	bool is_forward = !reverse;

	D_ASSERT(!filter_plan->apply.empty());
	auto &expr = filter_plan->apply[0];
	auto node_idx = expr.table_index;
	transfer_graph[node_idx]->Add(create_table, filter_plan, is_forward, true);
}

void TransferGraphManager::PrintTransferPlan() {
	// Output table groups
	unordered_set<JoinKeyTableGroup *> visited;
	for (auto &pair : table_groups) {
		auto &group = pair.second;
		if (visited.count(group.get())) {
			continue;
		}
		visited.insert(group.get());
		group->Print();
	}

	// Local helper to get operator name
	auto GetName = [](LogicalOperator &op) -> string {
		string ret;
		auto params = op.ParamsToString();

		if (params.contains("Table")) {
			ret = params.at("Table");
		} else {
			ret = "Unknown";
		}

		if (op.type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = op.Cast<LogicalGet>();
			if (get.table_filters.filters.empty()) {
				ret += " (No Filter)";
			}
		}
		return ret;
	};

	std::cout << "digraph G {" << '\n';

	// Create a map to store outgoing neighbors for each LogicalOperator
	std::unordered_map<std::string, std::vector<std::pair<std::string, std::pair<int, int>>>> outgoing_neighbors;

	// Populate the outgoing_neighbors map by traversing the edges
	for (const auto &edge : selected_edges) {
		std::string left_name = GetName(edge->left_table);
		std::string right_name = GetName(edge->right_table);
		idx_t left_column_id = edge->left_binding.column_index;   // Get the column id for the left table
		idx_t right_column_id = edge->right_binding.column_index; // Get the column id for the right table

		// Check the protection flags and only allow outgoing edges if the table is not protected
		if (!edge->protect_right) {
			outgoing_neighbors[left_name].emplace_back(right_name, std::make_pair(left_column_id, right_column_id));
		}
		if (!edge->protect_left) {
			outgoing_neighbors[right_name].emplace_back(left_name, std::make_pair(right_column_id, left_column_id));
		}
	}

	// Output nodes (LogicalOperators) and their outgoing neighbors (edges)
	for (const auto &op : transfer_order) {
		std::string op_name = GetName(*op); // Use GetName function for operator name
		std::cout << "\t\"" << op_name << "\";\n";

		// Output edges to each neighbor (only outgoing edges)
		if (outgoing_neighbors.find(op_name) != outgoing_neighbors.end()) {
			for (const auto &neighbor : outgoing_neighbors[op_name]) {
				std::string neighbor_name = neighbor.first;
				int left_column_id = neighbor.second.first;
				int right_column_id = neighbor.second.second;

				// Print edge with column ids
				std::cout << "\t\t\"" << op_name << "\" -> \"" << neighbor_name << "\" [label=\"Column "
				          << left_column_id << " -> Column " << right_column_id << "\"];\n";
			}
		}
	}

	std::cout << "}"
	          << "\n";
}

void TransferGraphManager::ExtractEdgesInfo(const vector<reference<LogicalOperator>> &join_operators) {
	// Deduplicate join conditions
	unordered_set<hash_t> existed_set;
	auto ComputeConditionHash = [](const JoinCondition &cond) {
		return cond.left->Hash() + cond.right->Hash();
	};

	// Union-Find structures
	BindingParentMap binding_parents;
	BindingGroupMap group_map;

	for (auto &join_ref : join_operators) {
		auto &join = join_ref.get();

		if (join.type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN &&
		    join.type != LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			continue;
		}

		auto &comp_join = join.Cast<LogicalComparisonJoin>();
		D_ASSERT(comp_join.expressions.empty());

		for (auto &cond : comp_join.conditions) {
			// Only equal predicates between two column refs are supported
			if (cond.comparison != ExpressionType::COMPARE_EQUAL ||
			    cond.left->type != ExpressionType::BOUND_COLUMN_REF ||
			    cond.right->type != ExpressionType::BOUND_COLUMN_REF) {
				continue;
			}

			// Skip duplicate conditions
			hash_t hash = ComputeConditionHash(cond);
			if (!existed_set.insert(hash).second) {
				continue;
			}

			auto &left_expr = cond.left->Cast<BoundColumnRefExpression>();
			auto &right_expr = cond.right->Cast<BoundColumnRefExpression>();

			ColumnBinding left_binding = table_operator_manager.GetRenaming(left_expr.binding);
			ColumnBinding right_binding = table_operator_manager.GetRenaming(right_expr.binding);

			auto left_node = table_operator_manager.GetTableOperator(left_binding.table_index);
			auto right_node = table_operator_manager.GetTableOperator(right_binding.table_index);

			if (!left_node || !right_node) {
				continue;
			}

			// Create edge
			auto edge =
			    make_shared_ptr<EdgeInfo>(cond.left->return_type, *left_node, left_binding, *right_node, right_binding);

			// Set edge protection flags based on join type
			switch (comp_join.type) {
			case LogicalOperatorType::LOGICAL_COMPARISON_JOIN:
				switch (comp_join.join_type) {
				case JoinType::LEFT:
					edge->protect_left = true;
					break;
				case JoinType::RIGHT:
					edge->protect_right = true;
					break;
				case JoinType::MARK:
					edge->protect_right = true;
					break;
				case JoinType::INNER:
				case JoinType::SEMI:
				case JoinType::RIGHT_SEMI:
					break;
				default:
					continue; // Skip unsupported types
				}
				break;

			case LogicalOperatorType::LOGICAL_DELIM_JOIN:
				if (comp_join.delim_flipped == 0) {
					edge->protect_left = true;
				} else {
					edge->protect_right = true;
				}
				break;

			default:
				continue;
			}

			// Store bidirectional edge
			neighbor_matrix[left_binding.table_index][right_binding.table_index] = edge;
			neighbor_matrix[right_binding.table_index][left_binding.table_index] = edge;

			// Merge groups if not protected
			if (!edge->protect_left && !edge->protect_right) {
				UnionBindings(left_binding, right_binding, cond.left->return_type, binding_parents, group_map);
			}
		}
	}

	// Finalize table_groups by resolving root bindings
	for (auto &entry : group_map) {
		ColumnBinding rep = FindBindingRoot(entry.first, binding_parents);
		table_groups[entry.first] = group_map[rep];
	}

	// Classify all tables into three categories: intermediate table, unfiltered table, and filtered table.
	ClassifyTables();
}

void TransferGraphManager::ClassifyTables() {
	for (auto &pair : table_operator_manager.table_operators) {
		auto id = pair.first;
		auto &table = pair.second;
		auto &edges = neighbor_matrix[id];
		auto &join_keys = table_join_keys[id];

		// Check intermediate table, which belongs to more than 2 groups
		unordered_set<JoinKeyTableGroup *> belong_groups;
		for (auto &sub_pair : edges) {
			auto &edge = sub_pair.second;
			auto &join_key_group = table_groups[edge->left_binding];
			belong_groups.insert(join_key_group.get());

			// Record all join keys of this table
			if (edge->left_binding.table_index == id) {
				join_keys.insert(edge->left_binding);
			} else {
				join_keys.insert(edge->right_binding);
			}
		}
		if (belong_groups.size() > 1) {
			intermediate_table.insert(id);
			continue;
		}

		// Check unfiltered table  
		if (table->type == LogicalOperatorType::LOGICAL_GET) {
			auto &get = table->Cast<LogicalGet>();
			if (get.table_filters.filters.empty()) {
				D_PRINTF("table_%llu has no table filters", (unsigned long long)id);
				D_PRINTF("  table_%llu marked as UNFILTERED (no table filters)", (unsigned long long)id);
				unfiltered_table.insert(id);
				continue;
			} else {
				D_PRINTF("  table_%llu has %zu table filters", (unsigned long long)id, get.table_filters.filters.size());
			}
		}

		D_PRINTF("Adding table_%llu to filtered table", (unsigned long long)id);
		// last, it is a filtered table
		filtered_table.insert(id);
	}
}

void TransferGraphManager::SkipUnfilteredTable(const vector<reference<LogicalOperator>> &joins) {
	// TODO: currently, we do not support skip unfiltered tables participating in outer join.
	for (auto& op: joins) {
		auto &join = op.get().Cast<LogicalComparisonJoin>();
		if (join.join_type != JoinType::INNER) {
			return;
		}
	}

	D_PRINTF("SkipUnfilteredTable: found %zu unfiltered tables", unfiltered_table.size());
	for (auto table_idx : unfiltered_table) {
		D_PRINTF("  table_%llu", (unsigned long long)table_idx);
	}

	bool changed = false;
	do {
		changed = false;
		for (auto &table_idx : unfiltered_table) {
			// 2.1 collect received bfs
			unordered_map<idx_t, vector<shared_ptr<EdgeInfo>>> received_bfs;
			auto &edges = neighbor_matrix[table_idx];

			for (auto &pair : edges) {
				auto &e = pair.second;

				if (e->left_binding.table_index == table_idx && !e->protect_left) {
					auto &bfs = received_bfs[e->left_binding.column_index];
					bfs.push_back(e);
				} else if (e->right_binding.table_index == table_idx && !e->protect_right) {
					auto &bfs = received_bfs[e->right_binding.column_index];
					bfs.push_back(e);
				}
			}

			// 2.2 remove BFs creation from this table
			for (auto &pair : edges) {
				auto &edge = pair.second;

				bool is_left = (edge->left_binding.table_index == table_idx && !edge->protect_right);
				bool is_right = (edge->right_binding.table_index == table_idx && !edge->protect_left);
				if (!is_left && !is_right) {
					continue;
				}

				idx_t col_idx = is_left ? edge->left_binding.column_index : edge->right_binding.column_index;
				auto &bfs = received_bfs[col_idx];

				// 2.2.1 add new links
				for (auto &bf_edge : bfs) {
					// the same edge
					if (bf_edge->left_binding == edge->left_binding && bf_edge->right_binding == edge->right_binding) {
						continue;
					}

					bool bf_left = (bf_edge->left_binding.table_index == table_idx && !bf_edge->protect_left);
					bool bf_right = (bf_edge->right_binding.table_index == table_idx && !bf_edge->protect_right);
					if (!bf_left && !bf_right) {
						continue;
					}

					shared_ptr<EdgeInfo> concat_edge = nullptr;
					if (is_left && bf_left) {
						concat_edge =
						    make_shared_ptr<EdgeInfo>(edge->return_type, bf_edge->right_table, bf_edge->right_binding,
						                              edge->right_table, edge->right_binding);
						concat_edge->protect_left = true;
					} else if (is_left && bf_right) {
						concat_edge =
						    make_shared_ptr<EdgeInfo>(edge->return_type, bf_edge->left_table, bf_edge->left_binding,
						                              edge->right_table, edge->right_binding);
						concat_edge->protect_left = true;
					} else if (is_right && bf_left) {
						concat_edge = make_shared_ptr<EdgeInfo>(edge->return_type, edge->left_table, edge->left_binding,
						                                        bf_edge->right_table, bf_edge->right_binding);
						concat_edge->protect_right = true;
					} else if (is_right && bf_right) {
						concat_edge = make_shared_ptr<EdgeInfo>(edge->return_type, edge->left_table, edge->left_binding,
						                                        bf_edge->left_table, bf_edge->left_binding);
						concat_edge->protect_right = true;
					}

					if (concat_edge) {
						idx_t i = concat_edge->left_binding.table_index;
						idx_t j = concat_edge->right_binding.table_index;
						auto &edge_ij = neighbor_matrix[i][j];
						auto &edge_ji = neighbor_matrix[j][i];

						bool exists = false;
						if (edge_ij != nullptr) {
							bool same_direction = edge_ij->left_binding == concat_edge->left_binding &&
							                      edge_ij->right_binding == concat_edge->right_binding;
							bool reverse_direction = edge_ij->left_binding == concat_edge->right_binding &&
							                         edge_ij->right_binding == concat_edge->left_binding;

							if (same_direction || reverse_direction) {
								if (same_direction) {
									edge_ij->protect_left &= concat_edge->protect_left;
									edge_ij->protect_right &= concat_edge->protect_right;
								} else { // reverse_direction
									edge_ij->protect_left &= concat_edge->protect_right;
									edge_ij->protect_right &= concat_edge->protect_left;
								}
								exists = true;
							}
						}

						if (!exists) {
							edge_ij = concat_edge;
							edge_ji = concat_edge;
						}
					}
				}

				// 2.2.2 disable current link
				if (is_left) {
					edge->protect_right = true;
				} else {
					edge->protect_left = true;
				}

				changed = true;
			}

			// 2.3. Remove invalid links
			for (auto it = edges.begin(); it != edges.end();) {
				auto &edge = it->second;

				// If the condition is met, erase the item from the unordered_map
				if (edge->protect_left && edge->protect_right) {
					it = edges.erase(it);
				} else {
					++it;
				}
			}
		}
	} while (changed);
}

void TransferGraphManager::LargestRoot(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<idx_t> constructed_set, unconstructed_set;
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;
	idx_t root = std::numeric_limits<idx_t>::max();

	// Initialize nodes
	for (auto &entry : table_operator_manager.table_operators) {
		idx_t id = entry.first;
		auto node = make_uniq<GraphNode>(id, prior_flag--);

		if (entry.second == sorted_nodes.back()) {
			root = id;
			constructed_set.insert(id);
		} else {
			unconstructed_set.insert(id);
		}

		transfer_graph[id] = std::move(node);
	}

	// Add root
	transfer_order.push_back(table_operator_manager.GetTableOperator(root));
	table_operator_manager.table_operators.erase(root);

	// Build graph
	while (!unconstructed_set.empty()) {
		auto selected_edge = FindEdge(constructed_set, unconstructed_set);
		if (selected_edge.first == std::numeric_limits<idx_t>::max()) {
			break;
		}

		auto &edge = neighbor_matrix[selected_edge.first][selected_edge.second];
		selected_edges.emplace_back(std::move(edge));

		auto node = transfer_graph[selected_edge.second].get();
		node->cardinality_order = prior_flag--;

		transfer_order.push_back(table_operator_manager.GetTableOperator(node->id));
		table_operator_manager.table_operators.erase(node->id);

		unconstructed_set.erase(selected_edge.second);
		constructed_set.insert(selected_edge.second);
	}
}
void TransferGraphManager::LargestRootUpdated(vector<LogicalOperator *> &sorted_nodes) {
	unordered_set<idx_t> constructed_set, unconstructed_set;
	int prior_flag = static_cast<int>(table_operator_manager.table_operators.size()) - 1;
	idx_t root = std::numeric_limits<idx_t>::max();

#ifdef DEBUG
	Printer::Print("Sorted nodes order - descending order:");
	string nodes_str = "  ";
	for (auto it = sorted_nodes.rbegin(); it != sorted_nodes.rend(); ++it) {
		auto &node = *it;
		idx_t table_idx = table_operator_manager.GetScalarTableIndex(node);
		nodes_str += std::to_string(table_idx) + " ";
	}
	Printer::Print(nodes_str);
#endif

	root = table_operator_manager.GetScalarTableIndex(sorted_nodes.back());
	D_PRINTF("Root = %llu", (unsigned long long)root);

	// Try to choose the largest filtered or intermediate table as the root
	// for (auto it = sorted_nodes.rbegin(); it != sorted_nodes.rend(); ++it) {
	// 	auto &node = *it;
	// 	auto id = table_operator_manager.GetScalarTableIndex(node);
	// 	if (filtered_table.count(id) || intermediate_table.count(id)) {
	// 		std::cout << "\nRoot = " << id << std::endl;
	// 		root = id;
	// 		break;
	// 	}
	// }

	// If we cannot find it, use the largest table as the root
	if (root == std::numeric_limits<idx_t>::max()) {
		auto &node = sorted_nodes.back();
		root = table_operator_manager.GetScalarTableIndex(node);
	}

	D_PRINTF("LargestRootUpdated: selected root = table_%llu", (unsigned long long)root);
	D_PRINTF("filtered_table.size()=%zu, intermediate_table.size()=%zu",
	         filtered_table.size(), intermediate_table.size());

	// Initialize nodes
	for (auto &entry : table_operator_manager.table_operators) {
		idx_t id = entry.first;
		if (id == root) {
			constructed_set.insert(id);
		} else {
			unconstructed_set.insert(id);
		}

		auto node = make_uniq<GraphNode>(id, prior_flag--);
		transfer_graph[id] = std::move(node);
	}

	// Add root
	transfer_order.push_back(table_operator_manager.GetTableOperator(root));
	table_operator_manager.table_operators.erase(root);
	for (auto &col_binding : table_join_keys[root]) {
		auto &group = table_groups[col_binding];
		if (group) {
			group->RegisterLeader(root, col_binding);
		}
	}

	// Build graph
	while (!unconstructed_set.empty()) {
		auto selected_edge = FindEdge(constructed_set, unconstructed_set);
		if (selected_edge.first == std::numeric_limits<idx_t>::max()) {
			break;
		}

		D_PRINTF("  spanning tree edge: table_%llu <-> table_%llu",
		         (unsigned long long)selected_edge.first, (unsigned long long)selected_edge.second);

		auto &edge = neighbor_matrix[selected_edge.first][selected_edge.second];
		selected_edges.emplace_back(std::move(edge));

		auto node = transfer_graph[selected_edge.second].get();
		node->cardinality_order = prior_flag--;

		transfer_order.push_back(table_operator_manager.GetTableOperator(node->id));
		table_operator_manager.table_operators.erase(node->id);
		for (auto &col_binding : table_join_keys[node->id]) {
			auto &group = table_groups[col_binding];
			if (group) {
				group->RegisterLeader(node->id, col_binding);
			}
		}

		unconstructed_set.erase(selected_edge.second);
		constructed_set.insert(selected_edge.second);
	}
}

void TransferGraphManager::CreateOriginTransferPlan() {
	auto saved_nodes = table_operator_manager.table_operators;
	while (!table_operator_manager.table_operators.empty()) {
		LargestRoot(table_operator_manager.sorted_table_operators);
		table_operator_manager.SortTableOperators();
	}
	table_operator_manager.table_operators = saved_nodes;

	for (auto &edge : selected_edges) {
		if (!edge) {
			continue;
		}

		idx_t left_idx = TableOperatorManager::GetScalarTableIndex(&edge->left_table);
		idx_t right_idx = TableOperatorManager::GetScalarTableIndex(&edge->right_table);

		D_ASSERT(left_idx != std::numeric_limits<idx_t>::max() && right_idx != std::numeric_limits<idx_t>::max());

		auto &type = edge->return_type;
		auto left_node = transfer_graph[left_idx].get();
		auto right_node = transfer_graph[right_idx].get();

		auto left_cols = edge->left_binding;
		auto right_cols = edge->right_binding;

		auto protect_left = edge->protect_left;
		auto protect_right = edge->protect_right;

		// smaller table is in the left
		if (left_node->cardinality_order > right_node->cardinality_order) {
			std::swap(left_node, right_node);
			std::swap(left_cols, right_cols);
			std::swap(protect_left, protect_right);
		}

		// forward: from the smaller to the larger
		if (!protect_right) {
			left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, true, false);
			right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, true, true);
		}

		// backward: from the larger to the smaller
		if (!protect_left) {
			left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, false, true);
			right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, false, false);
		}
	}
}

void TransferGraphManager::CreateTransferPlanUpdated() {
	D_PRINT("\n=== CREATE TRANSFER PLAN UPDATED ===");
	
	auto saved_nodes = table_operator_manager.table_operators;
	D_PRINT("calling LargestRootUpdated to build spanning tree...");
	
	while (!table_operator_manager.table_operators.empty()) {
		LargestRootUpdated(table_operator_manager.sorted_table_operators);
		table_operator_manager.SortTableOperators();
	}
	table_operator_manager.table_operators = saved_nodes;

	D_PRINTF("selected_edges size: %zu", selected_edges.size());
	D_PRINTF("transfer_order size: %zu", transfer_order.size());
	
#ifdef DEBUG
	string order_str = "transfer_order: ";
	for (auto *op : transfer_order) {
		auto table_idx = TableOperatorManager::GetScalarTableIndex(op);
		order_str += "table_" + std::to_string(table_idx) + " ";
	}
	Printer::Print(order_str);
#endif

	D_PRINT("processing selected edges to build transfer graph...");
	for (size_t i = 0; i < selected_edges.size(); i++) {
		auto &edge = selected_edges[i];
		if (!edge) {
			D_PRINTF("  edge %zu: null, skipping", i);
			continue;
		}

		idx_t left_idx = TableOperatorManager::GetScalarTableIndex(&edge->left_table);
		idx_t right_idx = TableOperatorManager::GetScalarTableIndex(&edge->right_table);

		D_ASSERT(left_idx != std::numeric_limits<idx_t>::max() && right_idx != std::numeric_limits<idx_t>::max());

		auto &type = edge->return_type;
		auto left_node = transfer_graph[left_idx].get();
		auto right_node = transfer_graph[right_idx].get();

		auto left_cols = edge->left_binding;
		auto right_cols = edge->right_binding;

		auto protect_left = edge->protect_left;
		auto protect_right = edge->protect_right;

		D_PRINTF("  edge %zu: table_%llu (cardinality_order=%d) <-> table_%llu (cardinality_order=%d)",
		         i, (unsigned long long)left_idx, left_node->cardinality_order, 
		         (unsigned long long)right_idx, right_node->cardinality_order);
		D_PRINTF("    protect_left=%s, protect_right=%s", 
		         protect_left ? "true" : "false", protect_right ? "true" : "false");

		// smaller table is in the left
		if (left_node->cardinality_order > right_node->cardinality_order) {
			D_PRINT("    swapping order: left becomes right, right becomes left");
			std::swap(left_node, right_node);
			std::swap(left_cols, right_cols);
			std::swap(protect_left, protect_right);
		}

		// forward: from the smaller to the larger
		if (!protect_right) {
			D_PRINTF("    adding FORWARD edges: table_%llu -> table_%llu", 
			         (unsigned long long)left_node->id, (unsigned long long)right_node->id);
			left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, true, false);
			right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, true, true);
		} else {
			D_PRINT("    skipping forward edges (protect_right=true)");
		}

		// backward: from the larger to the smaller
		if (!protect_left) {
			auto &group = table_groups[right_cols];
			if (group) {
				D_PRINTF("    adding BACKWARD edges with GROUP LEADER: table_%llu -> group_leader_%llu", 
				         (unsigned long long)left_node->id, (unsigned long long)group->leader_id);
				auto group_leader = group->leader_id;
				auto &leader_cols = group->leader_column_binding;
				auto leader = transfer_graph[group_leader].get();

				left_node->Add(group_leader, {left_cols}, {leader_cols}, {type}, false, true);
				leader->Add(left_node->id, {left_cols}, {leader_cols}, {type}, false, false);
			} else {
				D_PRINTF("    adding BACKWARD edges: table_%llu -> table_%llu", 
				         (unsigned long long)left_node->id, (unsigned long long)right_node->id);
				left_node->Add(right_node->id, {left_cols}, {right_cols}, {type}, false, true);
				right_node->Add(left_node->id, {left_cols}, {right_cols}, {type}, false, false);
			}
		} else {
			D_PRINT("    skipping backward edges (protect_left=true)");
		}
	}

	// print final transfer graph
#ifdef DEBUG
	Printer::Print("\nfinal transfer graph:");
	for (auto &pair : transfer_graph) {
		auto &node = *pair.second;
		Printer::PrintF("  table_%llu (cardinality_order=%d):", (unsigned long long)node.id, node.cardinality_order);
		
		string fwd_out = "    forward out edges: ";
		for (auto &edge : node.forward_stage_edges.out) {
			fwd_out += "->" + std::to_string(edge->destination) + " ";
		}
		Printer::Print(fwd_out);
		
		string fwd_in = "    forward in edges: ";
		for (auto &edge : node.forward_stage_edges.in) {
			fwd_in += "<-" + std::to_string(edge->destination) + " ";
		}
		Printer::Print(fwd_in);
		
		string bwd_out = "    backward out edges: ";
		for (auto &edge : node.backward_stage_edges.out) {
			bwd_out += "->" + std::to_string(edge->destination) + " ";
		}
		Printer::Print(bwd_out);
		
		string bwd_in = "    backward in edges: ";
		for (auto &edge : node.backward_stage_edges.in) {
			bwd_in += "<-" + std::to_string(edge->destination) + " ";
		}
		Printer::Print(bwd_in);
	}
	
	Printer::Print("=== END CREATE TRANSFER PLAN UPDATED ===\n");
#endif
}

pair<idx_t, idx_t> TransferGraphManager::FindEdge(const unordered_set<idx_t> &constructed_set,
                                                  const unordered_set<idx_t> &unconstructed_set) {
	pair<idx_t, idx_t> result {std::numeric_limits<idx_t>::max(), std::numeric_limits<idx_t>::max()};
	idx_t max_cardinality = 0;
	bool is_indirected = false;

	for (auto i : unconstructed_set) {
		for (auto j : constructed_set) {
			auto &edge = neighbor_matrix[j][i];
			if (edge == nullptr) {
				continue;
			}

			idx_t cardinality = table_operator_manager.GetTableOperator(i)->estimated_cardinality;
			if (cardinality > max_cardinality ||
			    (is_indirected == false && !edge->protect_left && !edge->protect_right)) {
				max_cardinality = cardinality;
				result = {j, i};
				is_indirected = !edge->protect_left && !edge->protect_right;
			}
		}
	}
	return result;
}

} // namespace duckdb
