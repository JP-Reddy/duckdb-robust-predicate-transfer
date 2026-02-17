#include "dag_printer.hpp"
#include "../optimizer/rpt_optimizer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// rendered subtree: lines of text + horizontal center position
struct RenderedBlock {
	vector<string> lines;
	int center; // column where the connector attaches
};

static string FormatCardinality(idx_t card) {
	if (card >= 1000000000) {
		return StringUtil::Format("%.1fB rows", (double)card / 1e9);
	} else if (card >= 1000000) {
		return StringUtil::Format("%.1fM rows", (double)card / 1e6);
	} else if (card >= 1000) {
		return StringUtil::Format("%.1fK rows", (double)card / 1e3);
	}
	return std::to_string(card) + " rows";
}

static RenderedBlock MakeBox(const string &name_line, const string &card_line) {
	idx_t inner_width = std::max(name_line.size(), card_line.size());
	string top    = "+" + string(inner_width + 2, '-') + "+";
	string mid1   = "| " + name_line + string(inner_width - name_line.size(), ' ') + " |";
	string mid2   = "| " + card_line + string(inner_width - card_line.size(), ' ') + " |";
	string bottom = "+" + string(inner_width + 2, '-') + "+";

	RenderedBlock block;
	block.lines = {top, mid1, mid2, bottom};
	block.center = (int)(top.size() / 2);
	return block;
}

static RenderedBlock RenderSubtree(TreeNode *node, TableManager &table_mgr) {
	string table_name = table_mgr.GetTableName(node->table_idx);
	string name_line = table_name + " (table " + std::to_string(node->table_idx) + ")";
	string card_line = FormatCardinality(node->table_op->estimated_cardinality);

	RenderedBlock parent_box = MakeBox(name_line, card_line);

	if (node->children.empty()) {
		return parent_box;
	}

	// render all children
	vector<RenderedBlock> child_blocks;
	vector<string> edge_labels;
	for (auto *child : node->children) {
		child_blocks.push_back(RenderSubtree(child, table_mgr));

		// build edge label: parent_col / child_col
		JoinEdge *edge = child->edge_to_parent;
		string label;
		if (edge) {
			vector<ColumnBinding> parent_cols, child_cols;
			if (edge->table_a == node->table_idx) {
				parent_cols = edge->join_columns_a;
				child_cols = edge->join_columns_b;
			} else {
				parent_cols = edge->join_columns_b;
				child_cols = edge->join_columns_a;
			}
			for (idx_t i = 0; i < parent_cols.size(); i++) {
				if (i > 0) label += ", ";
				label += table_mgr.GetColumnName(node->table_idx, parent_cols[i].column_index);
				label += " / ";
				label += table_mgr.GetColumnName(child->table_idx, child_cols[i].column_index);
			}
		}
		edge_labels.push_back(label);
	}

	// place children side by side with gap
	const int gap = 4;
	int total_width = 0;
	vector<int> child_offsets;
	for (idx_t i = 0; i < child_blocks.size(); i++) {
		child_offsets.push_back(total_width);
		int block_width = 0;
		for (auto &line : child_blocks[i].lines) {
			block_width = std::max(block_width, (int)line.size());
		}
		total_width += block_width;
		if (i + 1 < child_blocks.size()) {
			total_width += gap;
		}
	}

	// compute child centers in combined coordinate space
	vector<int> child_centers;
	for (idx_t i = 0; i < child_blocks.size(); i++) {
		child_centers.push_back(child_offsets[i] + child_blocks[i].center);
	}

	// expand total_width if any edge label would be clipped
	for (idx_t i = 0; i < child_centers.size(); i++) {
		int label_start = child_centers[i] - (int)edge_labels[i].size() / 2;
		int label_end = label_start + (int)edge_labels[i].size();
		if (label_end > total_width) {
			total_width = label_end;
		}
		if (label_start < 0) {
			int shift = -label_start;
			for (auto &c : child_offsets) c += shift;
			for (auto &c : child_centers) c += shift;
			total_width += shift;
		}
	}

	// position parent box centered above children
	int children_mid = (child_centers.front() + child_centers.back()) / 2;
	int parent_width = (int)parent_box.lines[0].size();
	int parent_offset = children_mid - parent_width / 2;
	if (parent_offset < 0) {
		int shift = -parent_offset;
		for (auto &c : child_offsets) c += shift;
		for (auto &c : child_centers) c += shift;
		total_width += shift;
		parent_offset = 0;
	}
	total_width = std::max(total_width, parent_offset + parent_width);
	int parent_center = parent_offset + parent_width / 2;

	// build result
	RenderedBlock result;
	result.center = parent_center;

	// parent box lines
	for (auto &line : parent_box.lines) {
		string padded = string(parent_offset, ' ') + line;
		if ((int)padded.size() < total_width) {
			padded += string(total_width - padded.size(), ' ');
		}
		result.lines.push_back(padded);
	}

	// connector lines from parent to children
	if (child_blocks.size() == 1) {
		int cc = child_centers[0];
		string conn_line(total_width, ' ');
		if (cc >= 0 && cc < total_width) conn_line[cc] = '|';
		result.lines.push_back(conn_line);

		if (!edge_labels[0].empty()) {
			string label_line(total_width, ' ');
			int label_start = cc - (int)edge_labels[0].size() / 2;
			if (label_start < 0) label_start = 0;
			for (idx_t j = 0; j < edge_labels[0].size() && label_start + (int)j < total_width; j++) {
				label_line[label_start + j] = edge_labels[0][j];
			}
			result.lines.push_back(label_line);
		}

		string conn_line2(total_width, ' ');
		if (cc >= 0 && cc < total_width) conn_line2[cc] = '|';
		result.lines.push_back(conn_line2);
	} else {
		// horizontal branch line
		int leftmost = child_centers.front();
		int rightmost = child_centers.back();

		string branch_line(total_width, ' ');
		for (int c = leftmost; c <= rightmost; c++) {
			branch_line[c] = '-';
		}
		for (auto cc : child_centers) {
			if (cc >= 0 && cc < total_width) branch_line[cc] = '+';
		}
		if (parent_center >= 0 && parent_center < total_width) {
			branch_line[parent_center] = '+';
		}
		result.lines.push_back(branch_line);

		// edge labels row
		string label_line(total_width, ' ');
		for (idx_t i = 0; i < child_centers.size(); i++) {
			if (edge_labels[i].empty()) continue;
			int label_start = child_centers[i] - (int)edge_labels[i].size() / 2;
			if (label_start < 0) label_start = 0;
			for (idx_t j = 0; j < edge_labels[i].size() && label_start + (int)j < total_width; j++) {
				label_line[label_start + j] = edge_labels[i][j];
			}
		}
		result.lines.push_back(label_line);

		// vertical connectors to children
		string vert_line(total_width, ' ');
		for (auto cc : child_centers) {
			if (cc >= 0 && cc < total_width) vert_line[cc] = '|';
		}
		result.lines.push_back(vert_line);
	}

	// merge child blocks (pad shorter ones)
	idx_t max_child_height = 0;
	for (auto &cb : child_blocks) {
		max_child_height = std::max(max_child_height, (idx_t)cb.lines.size());
	}

	for (idx_t row = 0; row < max_child_height; row++) {
		string merged_line(total_width, ' ');
		for (idx_t i = 0; i < child_blocks.size(); i++) {
			if (row < child_blocks[i].lines.size()) {
				const string &src = child_blocks[i].lines[row];
				int offset = child_offsets[i];
				for (idx_t j = 0; j < src.size() && offset + (int)j < total_width; j++) {
					merged_line[offset + j] = src[j];
				}
			}
		}
		result.lines.push_back(merged_line);
	}

	return result;
}

void PrintTransferDAG(TreeNode *root, TableManager &table_mgr) {
	if (!root) {
		return;
	}

	RenderedBlock block = RenderSubtree(root, table_mgr);

	Printer::Print("\n=== DAG ===");
	for (auto &line : block.lines) {
		Printer::Print(line);
	}
	Printer::Print("=== DAG ===\n");
}

} // namespace duckdb
