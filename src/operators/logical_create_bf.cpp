#include "dag.hpp"
#include "logical_create_bf.hpp"
// #include "physical_hello.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "physical_create_bf.hpp"


#include <utility>

namespace duckdb {

LogicalCreateBF::LogicalCreateBF() : LogicalExtensionOperator() {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	message = "CREATE_BF";
}

LogicalCreateBF::LogicalCreateBF(vector<shared_ptr<FilterPlan>> filter_plans) 
    : LogicalExtensionOperator(), filter_plans(std::move(filter_plans)) {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	message = "CREATE_BF";
}

InsertionOrderPreservingMap<string> LogicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Operator"] = "LogicalCreateBF";
	return result;
}

vector<ColumnBinding> LogicalCreateBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalCreateBF::ResolveTypes() {
	types = children[0]->types;
}

PhysicalOperator &LogicalCreateBF::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	if (!physical) {
		auto &physical_op = generator.Make<PhysicalCreateBF>(filter_plans, types, estimated_cardinality);
		for (auto &child : children) {
			auto &child_physical = generator.CreatePlan(*child);
			physical_op.children.emplace_back(child_physical);
		}
		physical = &physical_op;
		return physical_op;
	}
	return *physical;
}

// void RegisterLogicalCreateBFOperatorExtension(DatabaseInstance &db) {
// 	auto &config = DBConfig::GetConfig(db);
// 	config.operator_extensions.push_back(make_uniq<LogicalCreateBF>());
// }
} // namespace duckdb