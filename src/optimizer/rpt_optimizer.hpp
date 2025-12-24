#pragma once

#include "graph_manager.hpp"
#include "table_manager.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

// tree node for rooted MST representation
struct TreeNode {
	idx_t table_idx;
	LogicalOperator* table_op;
	vector<TreeNode*> children;
	TreeNode* parent;
	int level; // distance from root (root = 0)
	JoinEdge* edge_to_parent; // null for root

	TreeNode(idx_t idx, LogicalOperator* op)
		: table_idx(idx), table_op(op), parent(nullptr), level(0), edge_to_parent(nullptr) {}
};

class RPTOptimizerContextState : public ClientContextState {
public:
	explicit RPTOptimizerContextState(ClientContext &context) {}

	vector<JoinEdge> join_edges;
//	map<table_id, idx_t> table_cardinalities;
	map<LogicalOperator *, idx_t> operator_to_table_id;

	TableManager table_mgr;
	vector<LogicalOperator*> join_ops;
	vector<JoinEdge> mst_edges;

	unordered_map<ColumnBinding, ColumnBinding, ColumnBindingHashFunction> rename_col_bindings;
public:
	// extract all the join edges from the plan
	//vector<JoinEdge> ExtractOperators(LogicalOperator &plan, vector<LogicalOperator*> &join_ops);
	vector<JoinEdge> ExtractOperators(LogicalOperator &plan);
	void ExtractOperatorsRecursive(LogicalOperator &plan, vector<LogicalOperator*> &join_ops);
	map<table_id, TableInfo> get_value();
	vector<JoinEdge> CreateJoinEdges(vector<LogicalOperator*> &join_ops);
	vector<JoinEdge> LargestRoot(vector<JoinEdge> &edges);

	// build rooted tree from MST edges with largest table as root
	TreeNode* BuildRootedTree(vector<JoinEdge> &mst_edges) const;

	// void CreateForwardPassModifications(LogicalOperator *smaller_table_op, LogicalOperator *larger_table_op,
	// 														const vector<ColumnBinding> &smaller_columns, const vector<ColumnBinding> &larger_columns,
	// 														unordered_map<LogicalOperator*, unique_ptr<LogicalOperator>> &forward_pass);
	//
	// void CreateBackwardPassModifications(LogicalOperator *smaller_table_op, LogicalOperator *larger_table_op,
	// 														const vector<ColumnBinding> &smaller_columns, const vector<ColumnBinding> &larger_columns,
	// 														unordered_map<LogicalOperator*, unique_ptr<LogicalOperator>> &backward_pass);
	//
	std::pair<unordered_map<LogicalOperator*, vector<BloomFilterOperation>>,
			unordered_map<LogicalOperator*, vector<BloomFilterOperation>>>
	GenerateStageModifications(const vector<JoinEdge> &mst_edges);

	unique_ptr<LogicalOperator> BuildStackedBFOperators(unique_ptr<LogicalOperator> base_plan,
							     const vector<BloomFilterOperation> &bf_ops,
							     bool reverse_order = false);

	unique_ptr<LogicalOperator> ApplyStageModifications(unique_ptr<LogicalOperator> plan,
							   const unordered_map<LogicalOperator*, vector<BloomFilterOperation>> &forward_bf_ops,
							   const unordered_map<LogicalOperator*, vector<BloomFilterOperation>> &backward_bf_ops);

	// helper to link USE_BF operators to their corresponding CREATE_BF operators
	void LinkUseBFToCreateBF(LogicalOperator *plan);

	// resolve column binding through rename chain to get base table binding
	ColumnBinding ResolveColumnBinding(const ColumnBinding &binding) const;

	// debug functions
	void DebugPrintGraph(const vector<JoinEdge> &edges) const;
	void DebugPrintMST(const vector<JoinEdge> &mst_edges, const vector<BloomFilterOperation> &bf_operations);

	unique_ptr<LogicalOperator> PreOptimize(unique_ptr<LogicalOperator> plan);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	// entry point for extension framework
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

} // namespace duckdb


