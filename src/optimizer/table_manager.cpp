#include "table_manager.hpp"

namespace duckdb {

idx_t TableManager::GetScalarTableIndex(LogicalOperator *op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		return op->GetTableIndex()[0];
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		return GetScalarTableIndex(op->children[0].get());
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		return op->GetTableIndex()[1];
	}
	default:
		return std::numeric_limits<idx_t>::max();
	}
}

void TableManager::AddTableOperator(LogicalOperator *op) {
	// op->estimated_cardinality = op->EstimateCardinality(context);
	TableInfo tbl_info;
	tbl_info.estimated_cardinality = op->estimated_cardinality;
	tbl_info.table_idx = GetScalarTableIndex(op);
	tbl_info.table_op = op;
	if (table_idx != std::numeric_limits<idx_t>::max() && table_lookup.find(table_idx) == table_lookup.end()) {
		table_lookup[table_idx] = tbl_info;
	}
}

}
