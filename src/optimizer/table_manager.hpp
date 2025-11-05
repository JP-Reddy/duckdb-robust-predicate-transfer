
namespace duckdb {
typedef idx_t table_id;

struct TableInfo {
	table_id id;
	LogicalOperator* table_op;  // LogicalGet, LogicalFilter->LogicalGet, etc.
	idx_t cardinality;
}

class TableManager {
public:
	// for lookup by table_id
	map<table_id, TableInfo> table_lookup;
	// for ordered processing
	vector<TableInfo> table_ops;

public:
	void AddTable(const TableInfo &table);
	TableInfo* GetTable(idx_t table_id);

	idx_t GetScalarTableIndex(LogicalOperator *op);

	void AddTableOperator(LogicalOperator *op);
};

}
