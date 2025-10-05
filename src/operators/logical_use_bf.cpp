#include "logical_use_bf.hpp"

class PhysicalUseBF;
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
	// if (!physical) {
	// 	auto &plan = generator.CreatePlan(*children[0]);
	// 	auto &use_bf = generator.Make<PhysicalUseBF>(plan.types, estimated_cardinality, can_stop);
	// 	physical = static_cast<PhysicalUseBF *>(&use_bf); // Ensure safe raw pointer storage
	// 	use_bf.children.emplace_back(plan);
	// 	return use_bf; // Transfer ownership safely
	// }
	// return *physical; // Ensure correct ownership

	if (!physical) {
		physical = std::make_unique<PhysicalUseBF>(filter_plan, types);
		for (auto &child : children) {
			physical->children.push_back(std::move(child));
		}
	}

	return *physical;
}

// void RegisterLogicalUseBFOperatorExtension(DatabaseInstance &db) {
// 	auto &config = DBConfig::GetConfig(db);
// 	config.operator_extensions.push_back(make_uniq<LogicalUseBF>());
// }
} // namespace duckdb