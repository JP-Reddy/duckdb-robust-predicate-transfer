#include "logical_use_bf.hpp"
#include "physical_use_bf.hpp"

namespace duckdb {

LogicalUseBF::LogicalUseBF() : LogicalExtensionOperator() {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
}

LogicalUseBF::LogicalUseBF(shared_ptr<FilterPlan> filter_plan) 
    : LogicalExtensionOperator(), filter_plan(std::move(filter_plan)) {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
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
		auto &use_bf = generator.Make<PhysicalUseBF>(filter_plan, plan.types, estimated_cardinality);
		physical = &use_bf;
		use_bf.children.emplace_back(plan);
		return use_bf;
	}
	return *physical;
}

// void RegisterLogicalUseBFOperatorExtension(DatabaseInstance &db) {
// 	auto &config = DBConfig::GetConfig(db);
// 	config.operator_extensions.push_back(make_uniq<LogicalUseBF>());
// }
} // namespace duckdb