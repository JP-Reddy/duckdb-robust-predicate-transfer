#include "logical_create_bf.hpp"
#include "physical_hello.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"

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
	// For now, create a simple physical hello operator as placeholder
	// This is a hardcoded implementation to get compilation working
	auto physical_hello = make_uniq<PhysicalHello>(types, message);

	// Add children if any
	for (auto &child : children) {
		physical_hello->children.push_back(generator.CreatePlan(*child));
	}

	return *physical_hello.release();
}

// void RegisterLogicalCreateBFOperatorExtension(DatabaseInstance &db) {
// 	auto &config = DBConfig::GetConfig(db);
// 	config.operator_extensions.push_back(make_uniq<LogicalCreateBF>());
// }
} // namespace duckdb