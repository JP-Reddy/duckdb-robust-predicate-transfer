#pragma once

#include "graph_manager.hpp"
#include "table_manager.hpp"
#include "duckdb/main/client_context_state.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

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
	vector<JoinEdge> CreateJoinEdges(vector<LogicalOperator*> &join_ops);
	vector<JoinEdge> LargestRoot(vector<JoinEdge> &edges);

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

	unique_ptr<LogicalOperator> BuildStackedBFOperators(LogicalOperator* table_op,
							     const vector<BloomFilterOperation> &bf_ops,
							     bool reverse_order = false);

	unique_ptr<LogicalOperator> ApplyStageModifications(unique_ptr<LogicalOperator> plan,
							   const unordered_map<LogicalOperator*, vector<BloomFilterOperation>> &forward_bf_ops,
							   const unordered_map<LogicalOperator*, vector<BloomFilterOperation>> &backward_bf_ops);
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


