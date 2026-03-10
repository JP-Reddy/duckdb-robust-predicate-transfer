#pragma once

#include "../optimizer/table_manager.hpp"
#include "duckdb/common/printer.hpp"

namespace duckdb {

struct TreeNode;

// tree utilities
TreeNode *FindNodeInTree(TreeNode *root, idx_t table_idx);
void SetTreeLevels(TreeNode *node, int level);

// render and print the RPT transfer DAG as an ASCII tree
void PrintTransferDAG(TreeNode *root, TableManager &table_mgr);
void PrintTransferDAG(TreeNode *root, TableManager &table_mgr, const string &title);

} // namespace duckdb
