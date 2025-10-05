#include "logical_use_bf.hpp"
#include "physical_use_bf.hpp"

namespace duckdb {

LogicalUseBF::LogicalUseBF() : LogicalExtensionOperator() {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	// message = "USE_BF";
}



InsertionOrderPreservingMap<string> LogicalUseBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Operator"] = "LogicalUseBF";
	return result;
}

vector<ColumnBinding> LogicalUseBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalUseBF::ResolveTypes() {
	types = children[0]->types;
}

PhysicalOperator &LogicalUseBF::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	if (!physical) {
		auto &plan = generator.CreatePlan(*children[0]);
		auto &use_bf = generator.Make<PhysicalUseBF>(filter_plan, plan.types);
		physical = static_cast<PhysicalUseBF*>(&use_bf);
		use_bf.children.emplace_back(plan);
		return static_cast<PhysicalOperator&>(use_bf);
	}
	return static_cast<PhysicalOperator&>(*physical);
}

// void RegisterLogicalUseBFOperatorExtension(DatabaseInstance &db) {
// 	auto &config = DBConfig::GetConfig(db);
// 	config.operator_extensions.push_back(make_uniq<LogicalUseBF>());
// }
} // namespace duckdb