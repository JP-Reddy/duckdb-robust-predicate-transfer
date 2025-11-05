#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator.hpp"


namespace duckdb {
typdef idx_t table_id;
	class JoinEdge {
		private:
			idx_t table1;
			idx_t table2;
			vector<ColumnBinding> join_columns_a;  // multi-column join support
			vector<ColumnBinding> join_columns_b;
			uint8_t weight;
			JoinType join_type;
			// reference<LogicalOperator> table1_op;
			// reference<LogicalOperator> table2_op;
	};

	struct TableInfo {
		table_id id;
		LogicalOperator* table_op;  // LogicalGet, LogicalFilter->LogicalGet, etc.
		idx_t cardinality;
	}
	class BloomFilterOperation {
		bool is_create; // true = CREATE_BF, false = USE_BF
		idx_t build_table;
		idx_t probe_table;
		vector<ColumnBinding> join_columns;
	};

}