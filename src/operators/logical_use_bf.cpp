#include "logical_use_bf.hpp"
#include "physical_use_bf.hpp"
#include "dag.hpp"

namespace duckdb {

LogicalUseBF::LogicalUseBF() : LogicalExtensionOperator() {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
}

LogicalUseBF::LogicalUseBF(const BloomFilterOperation &bf_op)
    : LogicalExtensionOperator(), bf_operation(bf_op) {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
}

InsertionOrderPreservingMap<string> LogicalUseBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Operator"] = "LogicalUseBF";
	
	result["Build Table"] = to_string(bf_operation.build_table_idx);
	result["Probe Table"] = to_string(bf_operation.probe_table_idx);

	string probe_cols = "";
	for (size_t i = 0; i < bf_operation.probe_columns.size(); i++) {
		if (i > 0) {
			probe_cols += ", ";
		}
		probe_cols += "(" + to_string(bf_operation.probe_columns[i].table_index) +
					 "." + to_string(bf_operation.probe_columns[i].column_index) + ")";
	}
	result["Probe Columns"] = probe_cols;
	
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

shared_ptr<FilterPlan> BloomFilterOperationToFilterPlan(const BloomFilterOperation &bf_op) {
	auto filter_plan = make_shared<FilterPlan>();
	filter_plan->build = bf_op.build_columns;
	filter_plan->apply = bf_op.probe_columns;
	// filter_plan->return_types will be populated later during ResolveTypes()
	return filter_plan;
}

PhysicalOperator &LogicalUseBF::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	if (!physical) {
		auto &plan = generator.CreatePlan(*children[0]);
		// TODO: Replace filter_plan with bf_operation everywhere.
		// this is a temp fix
		auto filter_plan = BloomFilterOperationToFilterPlan(bf_operation);
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