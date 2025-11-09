#pragma once

#include "duckdb/common/types.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include <map>


namespace duckdb {
typedef idx_t table_id;

struct TableInfo {
	table_id table_idx;
	LogicalOperator* table_op;  // LogicalGet, LogicalFilter->LogicalGet, etc.
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
	TableInfo* GetTableInfo(LogicalOperator *op);

	idx_t GetScalarTableIndex(LogicalOperator *op);

	void AddTableOperator(LogicalOperator *op);
};

} // namespace duckdb
