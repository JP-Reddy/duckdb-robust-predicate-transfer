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
	
	if (filter_plan) {
		result["Build Expressions"] = std::to_string(filter_plan->build.size());
		result["Apply Expressions"] = std::to_string(filter_plan->apply.size());
		result["Build Columns"] = std::to_string(filter_plan->bound_cols_build.size());
		result["Apply Columns"] = std::to_string(filter_plan->bound_cols_apply.size());
	} else {
		result["Filter Plan"] = "NULL";
	}
	
	if (related_create_bf) {
		result["Related CreateBF"] = "Present";
	} else {
		result["Related CreateBF"] = "NULL";
	}
	
	if (estimated_cardinality != DConstants::INVALID_INDEX) {
		result["Estimated Cardinality"] = std::to_string(estimated_cardinality);
	}
	
	return result;
}

vector<ColumnBinding> LogicalUseBF::GetColumnBindings() {
	return children[0]->GetColumnBindings();
}

void LogicalUseBF::ResolveTypes() {
	Printer::Print("Resolving types for LogicalUseBF");
	// if (!children.empty() && children[0]) {
		// Printer::Print("Resolving types for LogicalUseBF: children[0]");
		types = children[0]->types;
	// }
}

PhysicalOperator &LogicalUseBF::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	if (!physical) {
		auto &plan = generator.CreatePlan(*children[0]);
		auto &use_bf = generator.Make<PhysicalUseBF>(filter_plan, plan.types, estimated_cardinality);
		physical = static_cast<PhysicalUseBF*>(&use_bf);
		
		// Set up reference to related PhysicalCreateBF if available
		if (related_create_bf && related_create_bf->physical) {
			physical->related_create_bf = related_create_bf->physical;
		}
		
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