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
#include "../optimizer/graph_manager.hpp"

namespace duckdb {
class DatabaseInstance;
class PhysicalCreateBF;

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

	vector<shared_ptr<DynamicTableFilterSet>> min_max_to_create;
	vector<vector<ColumnBinding>> min_max_applied_cols;
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

// void RegisterLogicalCreateBFOperatorExtension(DatabaseInstance &instance);

} // namespace duckdb
