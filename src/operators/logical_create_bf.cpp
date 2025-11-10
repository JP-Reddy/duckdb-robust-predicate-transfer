#include "dag.hpp"
#include "logical_create_bf.hpp"
// #include "physical_hello.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "physical_create_bf.hpp"
#include "dag.hpp"

#include <utility>

namespace duckdb {

LogicalCreateBF::LogicalCreateBF() : LogicalExtensionOperator() {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	message = "CREATE_BF";
}

LogicalCreateBF::LogicalCreateBF(const BloomFilterOperation &bf_op)
    : LogicalExtensionOperator(), bf_operation(bf_op) {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	message = "CREATE_BF";
}

InsertionOrderPreservingMap<string> LogicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Operator"] = "LogicalCreateBF";
	result["Build Table"] = to_string(bf_operation.build_table_idx);
	result["Probe Table"] = to_string(bf_operation.probe_table_idx);

	string build_cols = "";
	for (size_t i = 0; i < bf_operation.build_columns.size(); i++) {
		if (i > 0) {
			build_cols += ", ";
		}
		build_cols += "(" + to_string(bf_operation.build_columns[i].table_index) +
					 "." + to_string(bf_operation.build_columns[i].column_index) + ")";
	}
	result["Build Columns"] = build_cols;
	
	if (estimated_cardinality != DConstants::INVALID_INDEX) {
		result["Estimated Cardinality"] = std::to_string(estimated_cardinality);
	}
	
	return result;
}

vector<ColumnBinding> LogicalCreateBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCreateBF::ResolveTypes() {
	if (!children.empty() && children[0]) {
		types = children[0]->types;
	}
}

shared_ptr<FilterPlan> BloomFilterOperationToFilterPlan(const BloomFilterOperation &bf_op) {
	auto filter_plan = make_shared<FilterPlan>();
	filter_plan->build = bf_op.build_columns;
	filter_plan->apply = bf_op.probe_columns;
	// filter_plan->return_types will be populated later during ResolveTypes()
	return filter_plan;
}

PhysicalOperator &LogicalCreateBF::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	if (!physical) {
		auto filter_plan = BloomFilterOperationToFilterPlan(bf_operation);
		auto &physical_op = generator.Make<PhysicalCreateBF>(filter_plan, types, estimated_cardinality);
		for (auto &child : children) {
			auto &child_physical = generator.CreatePlan(*child);
			physical_op.children.emplace_back(child_physical);
		}
		physical = static_cast<PhysicalCreateBF*>(&physical_op);
		return physical_op;
	}
	return *physical;
}

// void RegisterLogicalCreateBFOperatorExtension(DatabaseInstance &db) {
// 	auto &config = DBConfig::GetConfig(db);
// 	config.operator_extensions.push_back(make_uniq<LogicalCreateBF>());
// }
} // namespace duckdb