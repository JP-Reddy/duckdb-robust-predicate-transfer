#include "table_manager.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"

namespace duckdb {

// helper to get operator type name for debug
static const char* GetOpTypeName(LogicalOperatorType type) {
	switch (type) {
	case LogicalOperatorType::LOGICAL_GET: return "GET";
	case LogicalOperatorType::LOGICAL_FILTER: return "FILTER";
	case LogicalOperatorType::LOGICAL_PROJECTION: return "PROJECTION";
	case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: return "COMPARISON_JOIN";
	case LogicalOperatorType::LOGICAL_DELIM_JOIN: return "DELIM_JOIN";
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: return "AGGREGATE";
	case LogicalOperatorType::LOGICAL_WINDOW: return "WINDOW";
	case LogicalOperatorType::LOGICAL_UNION: return "UNION";
	case LogicalOperatorType::LOGICAL_CHUNK_GET: return "CHUNK_GET";
	case LogicalOperatorType::LOGICAL_DELIM_GET: return "DELIM_GET";
	default: return "OTHER";
	}
}

void TableManager::AddTable(const TableInfo &table) {
	table_lookup[table.table_idx] = table;
	table_ops.push_back(table);
}

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
		// handle FILTER cases like reference impl's GetTableIndexinFilter
		LogicalOperator *child = op->children[0].get();
		if (child->type == LogicalOperatorType::LOGICAL_GET) {
			// FILTER → GET: get table index from GET
			return child->Cast<LogicalGet>().GetTableIndex()[0];
		} else if (child->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN ||
		           child->type == LogicalOperatorType::LOGICAL_DELIM_JOIN) {
			// FILTER → JOIN (e.g., MARK join for IN clause): get table index from join's left child
			LogicalOperator *join_left = child->children[0].get();
			if (join_left->type == LogicalOperatorType::LOGICAL_GET) {
				return join_left->Cast<LogicalGet>().GetTableIndex()[0];
			}
			// recurse further if needed
			return GetScalarTableIndex(join_left);
		} else if (child->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
			return child->GetTableIndex()[0];
		}
		// default: recurse into child
		return GetScalarTableIndex(child);
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		return op->GetTableIndex()[1];
	}
	default:
		return std::numeric_limits<idx_t>::max();
	}
}

void TableManager::AddTableOperator(LogicalOperator *op) {
	TableInfo tbl_info;
	tbl_info.estimated_cardinality = op->estimated_cardinality;
	tbl_info.table_idx = GetScalarTableIndex(op);
	table_id table_idx = tbl_info.table_idx;
	tbl_info.table_op = op;

	if (table_idx != std::numeric_limits<idx_t>::max() && table_lookup.find(table_idx) == table_lookup.end()) {
		printf("[NODE_REG] AddTableOperator: type=%s, table_idx=%llu, cardinality=%llu, op=%p\n",
			   GetOpTypeName(op->type), table_idx, tbl_info.estimated_cardinality, (void*)op);
		table_lookup[table_idx] = tbl_info;
		table_ops.push_back(tbl_info);
	} else if (table_idx != std::numeric_limits<idx_t>::max()) {
		printf("[NODE_REG] AddTableOperator SKIPPED (already exists): type=%s, table_idx=%llu, op=%p\n",
			   GetOpTypeName(op->type), table_idx, (void*)op);
	}
}

TableInfo *TableManager::GetTableInfo(LogicalOperator *op) {
	if (!op) {
		return nullptr;
	}

	idx_t table_idx = GetScalarTableIndex(op);
	if(table_lookup.find(table_idx) == table_lookup.end()) {
		return nullptr;
	}
	return &table_lookup[table_idx];
}

} // namespace duckdb
