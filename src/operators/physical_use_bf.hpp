#include "duckdb.hpp"
#include "logical_create_bf.hpp"
#include "duckdb/execution/physical_operator.hpp"
// #include "duckdb/common/types/chunk_collection.hpp"
#include "logical_use_bf.hpp"
#include "dag.hpp"

using namespace duckdb;

class PhysicalUseBF : public PhysicalOperator {
public:
	static constexpr auto TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalUseBF(std::shared_ptr<FilterPlan> filter_plan, std::vector<LogicalType> types_p);

public:
	bool is_probing_side;
	std::vector<std::shared_ptr<FilterPlan>> filter_plans;
	idx_t estimated_cardinality;
};