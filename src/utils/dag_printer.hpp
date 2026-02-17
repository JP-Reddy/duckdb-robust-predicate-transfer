#pragma once

#include "../optimizer/table_manager.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

struct TreeNode;

// render and print the RPT transfer DAG as an ASCII tree
void PrintTransferDAG(TreeNode *root, TableManager &table_mgr);

} // namespace duckdb
