//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/operator/logical_create_bf.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "../optimizer/graph_manager.hpp"

namespace duckdb {
class DatabaseInstance;
class PhysicalCreateBF;
class LogicalUseBF;

class LogicalCreateBF : public LogicalExtensionOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	static constexpr auto OPERATOR_TYPE_NAME = "logical_create_bf";

public:
	explicit LogicalCreateBF();
	explicit LogicalCreateBF(const BloomFilterOperation &bf_op);

	bool can_stop = false;
	BloomFilterOperation bf_operation;
	PhysicalCreateBF *physical = nullptr;

	vector<LogicalUseBF *> related_use_bf;
	bool is_forward_pass = false;

	struct DynamicFilterTarget {
		shared_ptr<DynamicTableFilterSet> dynamic_filters;
		idx_t scan_column_index;
		ColumnBinding probe_column;
		LogicalType column_type;
		string column_name;
	};
	vector<DynamicFilterTarget> pushdown_targets;

	string message;

public:
	string GetExtensionName() const override {
		return "rpt";
	}
	InsertionOrderPreservingMap<string> ParamsToString() const override;
	vector<ColumnBinding> GetColumnBindings() override;

	PhysicalOperator &CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) override;

protected:
	void ResolveTypes() override;
};

} // namespace duckdb
