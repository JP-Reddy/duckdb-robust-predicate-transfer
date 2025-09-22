#include "dag.hpp"
#include "logical_create_bf.hpp"
// #include "physical_hello.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"



#include <utility>

namespace duckdb {

LogicalCreateBF::LogicalCreateBF() : LogicalExtensionOperator() {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	message = "CREATE_BF";
}



InsertionOrderPreservingMap<string> LogicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Operator"] = "LogicalCreateBF";
	return result;
}

vector<ColumnBinding> LogicalCreateBF::GetColumnBindings() {
	return {ColumnBinding(0, 0)};
}

void LogicalCreateBF::ResolveTypes() {
	types.emplace_back(LogicalType::VARCHAR);
}

PhysicalOperator &LogicalCreateBF::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	if (!physical) {
		auto &plan = generator.CreatePlan(*children[0]);
		auto &create_bf = generator.Make<PhysicalCreateBF>(plan.types, filter_plans, min_max_to_create,
												 min_max_applied_cols, estimated_cardinality, can_stop);
		physical = static_cast<PhysicalCreateBF *>(&create_bf); // Ensure safe raw pointer storage
		create_bf.children.emplace_back(plan);
		return create_bf; // Transfer ownership safely
	}
	return *physical; // Ensure correct ownership
}

// void RegisterLogicalCreateBFOperatorExtension(DatabaseInstance &db) {
// 	auto &config = DBConfig::GetConfig(db);
// 	config.operator_extensions.push_back(make_uniq<LogicalCreateBF>());
// }
} // namespace duckdb