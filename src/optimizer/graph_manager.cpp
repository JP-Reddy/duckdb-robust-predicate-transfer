#include "graph_manager.h"

JoinEdge::JoinEdge(LogicalOperator &join) {
	for(int i = 0; i < join.conditions.size(); i++) {
		if(join.conditions[i].comparison != ExpressionType::COMPARE_EQUAL
			|| join.conditions[i].left->type != ExpressionType::BOUND_COLUMN_REF
			|| join.conditions[i].right->type != ExpressionType::BOUND_COLUMN_REF) {
			continue;
			}
		hash_t hash = ComputeConditionHash(join.conditions[i]);
		if(!existed_set.insert(hash).second) {
			continue;
		}

}

GetScalarTableIndex(LogicalOperator *op) {
	switch (op->type) {
	case LogicalOperatorType::LOGICAL_WINDOW:
	case LogicalOperatorType::LOGICAL_CHUNK_GET:
	case LogicalOperatorType::LOGICAL_GET:
	case LogicalOperatorType::LOGICAL_DELIM_GET:
	case LogicalOperatorType::LOGICAL_PROJECTION:
	case LogicalOperatorType::LOGICAL_UNION:
	case LogicalOperatorType::LOGICAL_EXCEPT:
	case LogicalOperatorType::LOGICAL_INTERSECT: {
		return op->GetTableIndex()[0];
	}
	case LogicalOperatorType::LOGICAL_FILTER: {
		return GetScalarTableIndex(op->children[0].get());
	}
	case LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY: {
		return op->GetTableIndex()[1];
	}
	default:
		return std::numeric_limits<idx_t>::max();
	}
}

void TableOperatorManager::AddTableOperator(LogicalOperator *op) {
	// op->estimated_cardinality = op->EstimateCardinality(context);

	idx_t table_idx = GetScalarTableIndex(op);
	if (table_idx != std::numeric_limits<idx_t>::max() && table_operators.find(table_idx) == table_operators.end()) {
		table_operators[table_idx] = op;
	}
}