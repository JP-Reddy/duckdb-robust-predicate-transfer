#pragma once

#include "graph_manager.hpp"
#include "duckdb/main/client_context_state.hpp"

namespace duckdb {

class RPTOptimizerContextState : public ClientContextState {
public:
	explicit RPTOptimizerContextState(ClientContext &context) {}

	vector<JoinEdge> join_edges;
	map<idx_t, idx_t> table_cardinalities;
	map<LogicalOperator *, idx_t> operator_to_table_id;

	// extract all the join edges from the plan
	static vector<JoinEdge> ExtractJoins(LogicalOperator &plan);


	unique_ptr<LogicalOperator> PreOptimize(unique_ptr<LogicalOperator> plan);

	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> plan);

	// entry point for extension framework
	static void PreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
	static void Optimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan);
};

}


