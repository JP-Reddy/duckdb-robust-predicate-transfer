#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "logical_create_bf.hpp"
#include "logical_use_bf.hpp"
#include "physical_create_bf.hpp"
#include "physical_use_bf.hpp"
#include <utility>

namespace duckdb {

LogicalCreateBF::LogicalCreateBF() : LogicalExtensionOperator() {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	message = "CREATE_BF";
}

LogicalCreateBF::LogicalCreateBF(const BloomFilterOperation &bf_op) : LogicalExtensionOperator(), bf_operation(bf_op) {
	this->type = LogicalOperatorType::LOGICAL_EXTENSION_OPERATOR;
	message = "CREATE_BF";
}

InsertionOrderPreservingMap<string> LogicalCreateBF::ParamsToString() const {
	InsertionOrderPreservingMap<string> result;
	result["Operator"] = "LogicalCreateBF";
	result["Build Table"] = to_string(bf_operation.build_table_idx);
	// there can be multiple probe tables for a single create
	string probe_tables;
	vector<idx_t> seen_probe;
	for (const auto &col : bf_operation.probe_columns) {
		bool found = false;
		for (auto idx : seen_probe) {
			if (idx == col.table_index) {
				found = true;
				break;
			}
		}
		if (!found) {
			if (!probe_tables.empty())
				probe_tables += ", ";
			probe_tables += to_string(col.table_index);
			seen_probe.push_back(col.table_index);
		}
	}
	result["Probe Tables"] = probe_tables;

	string build_cols = "";
	for (size_t i = 0; i < bf_operation.build_columns.size(); i++) {
		if (i > 0) {
			build_cols += ", ";
		}
		build_cols += "(" + to_string(bf_operation.build_columns[i].table_index) + "." +
		              to_string(bf_operation.build_columns[i].column_index) + ")";
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

// shared_ptr<FilterPlan> BloomFilterOperationToFilterPlan(const BloomFilterOperation &bf_op) {
// 	auto filter_plan = make_shared<FilterPlan>();
// 	filter_plan->build = bf_op.build_columns;
// 	filter_plan->apply = bf_op.probe_columns;
// 	// filter_plan->return_types will be populated later during ResolveTypes()
// 	return filter_plan;
// }

PhysicalOperator &LogicalCreateBF::CreatePlan(ClientContext &context, PhysicalPlanGenerator &generator) {
	if (!physical) {
		// step 1: get child column bindings to understand chunk schema
		vector<ColumnBinding> child_bindings = children[0]->GetColumnBindings();

		// step 2: resolve/map the bf operation columns to chunk column indices.
		// resolved_indices stores the columns on which the bloom filters are
		// built.
		// TODO: optimize: Use a map for bf_operation.build_columns to speed up lookup
		vector<idx_t> resolved_indices;
		for (const ColumnBinding &column_binding : bf_operation.build_columns) {
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
		PhysicalOperator &physical_op = generator.Make<PhysicalCreateBF>(
		    make_shared_ptr<BloomFilterOperation>(bf_operation), types, estimated_cardinality, resolved_indices);
		// auto filter_plan = BloomFilterOperationToFilterPlan(bf_operation);
		// auto &physical_op = generator.Make<PhysicalCreateBF>(make_shared<BloomFilterOperation>(bf_operation), types,
		// estimated_cardinality);
		for (auto &child : children) {
			auto &child_physical = generator.CreatePlan(*child);
			physical_op.children.emplace_back(child_physical);
		}
		physical = static_cast<PhysicalCreateBF *>(&physical_op);

		// link back to related USE_BF operators
		// the links are used to create pipeline dependencies
		for (const LogicalUseBF *use_bf : related_use_bf) {
			if (use_bf->physical) {
				// TODO: keep either related_create_bf or related_create_bf_vec. Not both. Most likely we'll have to
				// remove related_create_bf.
				use_bf->physical->related_create_bf = physical;
				use_bf->physical->related_create_bf_vec.push_back(physical);
				// string probe_table = "table_" + std::to_string(use_bf->bf_operation.probe_table_idx);
				// string build_table = "table_" + std::to_string(bf_operation.build_table_idx);
				// Printer::Print(StringUtil::Format(
				// 	"[LOGICAL CREATE] linked back to USE_BF (probe=%s) from CREATE_BF (build=%s)",
				// 	probe_table.c_str(), build_table.c_str()));
			}
		}
		return physical_op;
	}
	return *physical;
}

// void RegisterLogicalCreateBFOperatorExtension(DatabaseInstance &db) {
// 	auto &config = DBConfig::GetConfig(db);
// 	config.operator_extensions.push_back(make_uniq<LogicalCreateBF>());
// }
} // namespace duckdb
