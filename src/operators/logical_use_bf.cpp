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
	if (!children.empty() && children[0]) {
		types = children[0]->types;
	}
}

PhysicalOperator &LogicalUseBF::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	if (!physical) {
		// step 1: get child column bindings to understand chunk schema
		vector<ColumnBinding> child_bindings = children[0]->GetColumnBindings();

		// step 2: resolve/map the bf operation probe columns to chunk column indices
		vector<idx_t> resolved_indices;
		for (const ColumnBinding &column_binding: bf_operation.probe_columns) {
			// find the position of the bf column ColumnBinding in the chunk columns
			for (idx_t i = 0; i < child_bindings.size(); i++) {
				if (child_bindings[i].table_index == column_binding.table_index &&
					child_bindings[i].column_index == column_binding.column_index) {
					resolved_indices.push_back(i);
					break;
				}
			}
		}

		// step 3: create physical operator with the resolved indices
		auto &plan = generator.CreatePlan(*children[0]);
		PhysicalOperator &physical_op = generator.Make<PhysicalUseBF>(
			make_shared_ptr<BloomFilterOperation>(bf_operation),
			plan.types,
			estimated_cardinality,
			resolved_indices);
		physical = static_cast<PhysicalUseBF*>(&physical_op);

		if (related_create_bf) {
			string probe_table = "table_" + std::to_string(bf_operation.probe_table_idx);
			Printer::Print(StringUtil::Format("[LOGICAL USE] probe table - %s Related_create_bf exists", probe_table.c_str()));
		}
		// set up reference to related PhysicalCreateBF if available
		if (related_create_bf && related_create_bf->physical) {
			string probe_table = "table_" + std::to_string(bf_operation.probe_table_idx);
			Printer::Print(StringUtil::Format("[LOGICAL USE] probe table - %s Related_create_bf  physical exists", probe_table.c_str()));
			physical->related_create_bf = related_create_bf->physical;
			physical->related_create_bf_vec.push_back(related_create_bf->physical);
		}

		physical_op.children.emplace_back(plan);
		return physical_op;
	}
	return *physical;
}

} // namespace duckdb