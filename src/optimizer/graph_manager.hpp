#pragma once

#include "duckdb/common/enums/join_type.hpp"
#include "duckdb/planner/column_binding.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/types.hpp"


namespace duckdb {
typedef idx_t table_id;
	class JoinEdge {
	public:
		idx_t table_a;
		idx_t table_b;
		vector<ColumnBinding> join_columns_a;  // multi-column join support
		vector<ColumnBinding> join_columns_b;
		idx_t weight;
		JoinType join_type;
		// reference<LogicalOperator> table1_op;
		// reference<LogicalOperator> table2_op;

	public:
		JoinEdge(table_id table_a, table_id table_b,
			 vector<ColumnBinding> cols_a, vector<ColumnBinding> cols_b,
			 idx_t weight, JoinType join_type)
		: table_a(table_a), table_b(table_b),
		  join_columns_a(std::move(cols_a)), join_columns_b(std::move(cols_b)),
		  weight(weight), join_type(join_type) {
			D_ASSERT(!join_columns_a.empty());
			D_ASSERT(join_columns_a.size() == join_columns_b.size());
		}
	};

	class BloomFilterOperation {
	public:
		bool is_create; // true = CREATE_BF, false = USE_BF
		idx_t build_table_idx;
		idx_t probe_table_idx;
		vector<ColumnBinding> join_columns;
		vector<ColumnBinding> build_columns;
		vector<ColumnBinding> probe_columns;
		JoinType join_type;
	};

} // namespace duckdb