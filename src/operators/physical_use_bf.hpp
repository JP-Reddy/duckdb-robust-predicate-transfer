#include "duckdb.hpp"
#include "logical_create_bf.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types/chunk_collection.hpp"
#include "operators/logical_use_bf.hpp"
#include "include/dag.hpp"

using namespace duckdb;

class PhysicalUseBF : public CachingPhysicalOperator {
public:
	static constexpr auto TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalUseBF(std::shared_ptr<FilterPlan> filter_plan, std::vector<LogicalType> types_p);

public:
	bool is_probing_side;
	std::vector<std::shared_ptr<FilterPlan>> filter_plans;
	idx_t estimated_cardinality;
};