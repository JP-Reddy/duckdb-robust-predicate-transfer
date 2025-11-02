//===----------------------------------------------------------------------===//
//                         DuckDB
//
// lib/dag.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "bloom_filter.hpp"

namespace duckdb {

struct FilterPlan {
	vector<ColumnBinding> build;
	vector<ColumnBinding> apply;
	vector<LogicalType> return_types;

	vector<idx_t> bound_cols_build;
	vector<idx_t> bound_cols_apply;

	void Serialize(Serializer &serializer) const;
	static unique_ptr<FilterPlan> Deserialize(Deserializer &deserializer);

	bool operator==(const FilterPlan &other) const;
};

class GraphEdge {
public:
	explicit GraphEdge(idx_t destination) : destination(destination) {
	}

	idx_t destination;

	// The left is the smaller table side, while the right is the bigger table side
	vector<ColumnBinding> left;
	vector<ColumnBinding> right;
	vector<LogicalType> return_types;
	vector<shared_ptr<FilterPlan>> filter_plan;
};

struct Edges {
	vector<unique_ptr<GraphEdge>> in;
	vector<unique_ptr<GraphEdge>> out;
};

class GraphNode {
public:
	GraphNode(idx_t id, int32_t priority) : id(id), cardinality_order(priority) {
	}

	idx_t id;
	int32_t cardinality_order;

	//! Predicate Transfer has two stages. The transfer graph is different because of the existence of LEFT JOIN, RIGHT
	//! JOIN, etc.
	Edges forward_stage_edges;
	Edges backward_stage_edges;

public:
	GraphEdge *Add(idx_t other, bool is_forward, bool is_in_edge);
	GraphEdge *Add(idx_t other, const vector<ColumnBinding> &left_cols, const vector<ColumnBinding> &right_cols,
	               const vector<LogicalType> &types, bool is_forward, bool is_in_edge);
	GraphEdge *Add(idx_t other, const shared_ptr<FilterPlan> &filter_plan, bool is_forward, bool is_in_edge);
};

using TransferGraph = unordered_map<idx_t, unique_ptr<GraphNode>>;
} // namespace duckdb
