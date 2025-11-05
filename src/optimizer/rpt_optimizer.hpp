#pragma once

#include "graph_manager.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {
typedef idx_t table_id;

class RPTOptimizerContextState : public ClientContextState {
public:
	explicit RPTOptimizerContextState(ClientContext &context) {}

	vector<JoinEdge> join_edges;
//	map<table_id, idx_t> table_cardinalities;
	map<LogicalOperator *, idx_t> operator_to_table_id;

	TableManager table_mgr;
	vector<LogicalOperator*> join_ops;

public:
	// extract all the join edges from the plan
	vector<JoinEdge> ExtractOperators(LogicalOperator &plan, vector<LogicalOperator*> &join_ops);


	unique_ptr<LogicalOperator> PreOptimize(unique_ptr<LogicalOperator> plan);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	// entry point for extension framework
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

}


