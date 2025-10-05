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
#include "dag.hpp"
#include "logical_create_bf.hpp"

namespace duckdb {
struct FilterPlan;
class DatabaseInstance;
class PhysicalUseBF;

class LogicalUseBF final : public LogicalExtensionOperator {
public:
	static constexpr auto TYPE = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	static constexpr auto OPERATOR_TYPE_NAME = "logical_use_bf";

public:
	explicit LogicalUseBF();

	shared_ptr<FilterPlan> filter_plan;
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
