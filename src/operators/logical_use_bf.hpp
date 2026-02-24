//===----------------------------------------------------------------------===//
//                         DuckDB
//
// operator/logical_use_bf.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "logical_create_bf.hpp"
#include "../optimizer/graph_manager.hpp"

namespace duckdb {
class DatabaseInstance;
class PhysicalUseBF;

class LogicalUseBF final : public LogicalExtensionOperator {
public:
	static constexpr const LogicalOperatorType TYPE = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	static constexpr auto OPERATOR_TYPE_NAME = "logical_use_bf";

public:
	explicit LogicalUseBF();
	explicit LogicalUseBF(const BloomFilterOperation &bf_op);

	BloomFilterOperation bf_operation;
	LogicalCreateBF *related_create_bf = nullptr;

	PhysicalUseBF *physical = nullptr;

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

// void RegisterLogicalUseBFOperatorExtension(DatabaseInstance &instance);

} // namespace duckdb
