#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include <map>

namespace duckdb {
typedef idx_t table_id;

struct TableInfo {
	table_id table_idx;
	LogicalOperator *table_op; // LogicalGet, LogicalFilter->LogicalGet, etc.
	idx_t estimated_cardinality;
};

class TableManager {
public:
	// for lookup by table_id
	std::map<table_id, TableInfo> table_lookup;
	// for ordered processing
	vector<TableInfo> table_ops;

public:
	void AddTable(const TableInfo &table);
	TableInfo *GetTableInfo(LogicalOperator *op);

	idx_t GetScalarTableIndex(LogicalOperator *op);

	void AddTableOperator(LogicalOperator *op);

	// navigate from registered operator to underlying LogicalGet
	static LogicalGet *FindLogicalGet(LogicalOperator *op);
	// resolve table name from table index
	string GetTableName(idx_t table_idx);
	// resolve column name from table index and column binding index
	string GetColumnName(idx_t table_idx, idx_t column_index);
};

} // namespace duckdb
