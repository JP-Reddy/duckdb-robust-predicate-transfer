#include "dag.hpp"

namespace duckdb {

bool FilterPlan::operator==(const FilterPlan &other) const {
	return build == other.build && apply == other.apply && return_types == other.return_types;
}

GraphEdge *GraphNode::Add(idx_t other, bool is_forward, bool is_in_edge) {
	auto &stage = (is_forward ? forward_stage_edges : backward_stage_edges);
	auto &edges = (is_in_edge ? stage.in : stage.out);
	for (auto &edge : edges) {
		if (edge->destination == other) {
			return edge.get();
		}
	}
	edges.emplace_back(make_uniq<GraphEdge>(other));
	return edges.back().get();
}

GraphEdge *GraphNode::Add(idx_t other, const vector<ColumnBinding> &left_cols, const vector<ColumnBinding> &right_cols,
						  const vector<LogicalType> &types, bool is_forward, bool is_in_edge) {
	auto *edge = Add(other, is_forward, is_in_edge);
	edge->left = left_cols;
	edge->right = right_cols;
	edge->return_types = types;
	return edge;
}

GraphEdge *GraphNode::Add(idx_t other, const shared_ptr<FilterPlan> &filter_plan, bool is_forward, bool is_in_edge) {
	auto *edge = Add(other, is_forward, is_in_edge);
	edge->filter_plan.push_back(filter_plan);
	return edge;
}

} // namespace duckdb
