#include "duckdb.hpp"
#include "logical_create_bf.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "logical_create_bf.hpp"
#include "dag.hpp"

using namespace duckdb;

class PhysicalCreateBF : public PhysicalOperator {
public:
	static constexpr auto TYPE = PhysicalOperatorType::EXTENSION;

public:
	PhysicalCreateBF(vector<LogicalType> types, const vector<shared_ptr<FilterPlan>> &filter_plans);

	// Required virtual methods
	virtual ~PhysicalCreateBF() = default;

	string GetName() const override;
	string ToString() const override;
	unique_ptr<PhysicalOperator> Clone() const override;

public:
	bool is_probing_side;
	std::vector<std::shared_ptr<FilterPlan>> filter_plans;
	idx_t estimated_cardinality;
};