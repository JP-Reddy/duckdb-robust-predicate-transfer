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
	string top = "+" + string(inner_width + 2, '-') + "+";
	string mid1 = "| " + name_line + string(inner_width - name_line.size(), ' ') + " |";
	string mid2 = "| " + card_line + string(inner_width - card_line.size(), ' ') + " |";
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
				if (i > 0)
					label += ", ";
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
			for (auto &c : child_offsets)
				c += shift;
			for (auto &c : child_centers)
				c += shift;
			total_width += shift;
		}
	}

	// position parent box centered above children
	int children_mid = (child_centers.front() + child_centers.back()) / 2;
	int parent_width = (int)parent_box.lines[0].size();
	int parent_offset = children_mid - parent_width / 2;
	if (parent_offset < 0) {
		int shift = -parent_offset;
		for (auto &c : child_offsets)
			c += shift;
		for (auto &c : child_centers)
			c += shift;
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
		if (cc >= 0 && cc < total_width)
			conn_line[cc] = '|';
		result.lines.push_back(conn_line);

		if (!edge_labels[0].empty()) {
			string label_line(total_width, ' ');
			int label_start = cc - (int)edge_labels[0].size() / 2;
			if (label_start < 0)
				label_start = 0;
			for (idx_t j = 0; j < edge_labels[0].size() && label_start + (int)j < total_width; j++) {
				label_line[label_start + j] = edge_labels[0][j];
			}
			result.lines.push_back(label_line);
		}

		string conn_line2(total_width, ' ');
		if (cc >= 0 && cc < total_width)
			conn_line2[cc] = '|';
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
			if (cc >= 0 && cc < total_width)
				branch_line[cc] = '+';
		}
		if (parent_center >= 0 && parent_center < total_width) {
			branch_line[parent_center] = '+';
		}
		result.lines.push_back(branch_line);

		// edge labels row
		string label_line(total_width, ' ');
		for (idx_t i = 0; i < child_centers.size(); i++) {
			if (edge_labels[i].empty())
				continue;
			int label_start = child_centers[i] - (int)edge_labels[i].size() / 2;
			if (label_start < 0)
				label_start = 0;
			for (idx_t j = 0; j < edge_labels[i].size() && label_start + (int)j < total_width; j++) {
				label_line[label_start + j] = edge_labels[i][j];
			}
		}
		result.lines.push_back(label_line);

		// vertical connectors to children
		string vert_line(total_width, ' ');
		for (auto cc : child_centers) {
			if (cc >= 0 && cc < total_width)
				vert_line[cc] = '|';
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

TreeNode *FindNodeInTree(TreeNode *root, idx_t table_idx) {
	if (!root) {
		return nullptr;
	}
	if (root->table_idx == table_idx) {
		return root;
	}
	for (auto *child : root->children) {
		auto *found = FindNodeInTree(child, table_idx);
		if (found) {
			return found;
		}
	}
	return nullptr;
}

void SetTreeLevels(TreeNode *node, int level) {
	if (!node) {
		return;
	}
	node->level = level;
	for (auto *child : node->children) {
		SetTreeLevels(child, level + 1);
	}
}

void PrintTransferDAG(TreeNode *root, TableManager &table_mgr) {
	PrintTransferDAG(root, table_mgr, "DAG");
}

void PrintTransferDAG(TreeNode *root, TableManager &table_mgr, const string &title) {
	if (!root) {
		return;
	}

	RenderedBlock block = RenderSubtree(root, table_mgr);

	Printer::Print("\n=== " + title + " ===");
	for (auto &line : block.lines) {
		Printer::Print(line);
	}
	Printer::Print("=== " + title + " ===\n");
}

void PrintPhysicalDAG(vector<PhysicalDAGNode *> &all_nodes, TableManager &table_mgr) {
	if (all_nodes.empty()) {
		return;
	}

	// group by level
	map<int, vector<PhysicalDAGNode *>> by_level;
	int max_level = 0;
	for (auto *node : all_nodes) {
		by_level[node->level].push_back(node);
		max_level = std::max(max_level, node->level);
	}

	// create boxes for all nodes
	map<PhysicalDAGNode *, RenderedBlock> boxes;
	for (auto *node : all_nodes) {
		string table_name = table_mgr.GetTableName(node->table_idx);
		string name_line = table_name + " (table " + std::to_string(node->table_idx) + ")";
		string card_line = FormatCardinality(node->table_op->estimated_cardinality);
		boxes[node] = MakeBox(name_line, card_line);
	}

	// compute level widths and box offsets within each level
	const int level_gap = 4;
	map<int, int> level_widths;
	map<int, vector<int>> level_offsets;

	for (int level = 0; level <= max_level; level++) {
		auto &nodes = by_level[level];
		int width = 0;
		level_offsets[level] = {};
		for (idx_t i = 0; i < nodes.size(); i++) {
			level_offsets[level].push_back(width);
			int box_width = 0;
			for (auto &line : boxes[nodes[i]].lines) {
				box_width = std::max(box_width, (int)line.size());
			}
			width += box_width;
			if (i + 1 < nodes.size()) {
				width += level_gap;
			}
		}
		level_widths[level] = width;
	}

	int total_width = 0;
	for (auto &entry : level_widths) {
		total_width = std::max(total_width, entry.second);
	}

	// center each level within total_width and record absolute node centers
	map<PhysicalDAGNode *, int> node_centers;

	for (int level = 0; level <= max_level; level++) {
		int shift = (total_width - level_widths[level]) / 2;
		auto &nodes = by_level[level];
		for (idx_t i = 0; i < nodes.size(); i++) {
			node_centers[nodes[i]] = shift + level_offsets[level][i] + boxes[nodes[i]].center;
		}
	}

	// render level by level
	vector<string> output;

	for (int level = 0; level <= max_level; level++) {
		auto &nodes = by_level[level];
		int shift = (total_width - level_widths[level]) / 2;

		// render boxes
		int max_height = 0;
		for (auto *node : nodes) {
			max_height = std::max(max_height, (int)boxes[node].lines.size());
		}

		for (int row = 0; row < max_height; row++) {
			string line(total_width, ' ');
			for (idx_t i = 0; i < nodes.size(); i++) {
				auto &box = boxes[nodes[i]];
				if (row < (int)box.lines.size()) {
					int offset = shift + level_offsets[level][i];
					for (idx_t j = 0; j < box.lines[row].size() && offset + (int)j < total_width; j++) {
						line[offset + j] = box.lines[row][j];
					}
				}
			}
			output.push_back(line);
		}

		// draw connectors to next level
		if (level >= max_level) {
			continue;
		}

		auto &children = by_level[level + 1];

		// collect parent->child connections for this level transition
		struct ConnInfo {
			int child_center;
			vector<int> parent_centers;
			vector<string> edge_labels;
		};
		vector<ConnInfo> conns;

		for (auto *child : children) {
			ConnInfo info;
			info.child_center = node_centers[child];

			for (idx_t pi = 0; pi < child->parents.size(); pi++) {
				auto *parent = child->parents[pi];
				if (parent->level != level) {
					continue;
				}
				info.parent_centers.push_back(node_centers[parent]);

				if (pi < child->edges_to_parents.size()) {
					auto &edge = child->edges_to_parents[pi];
					string label;
					for (idx_t ci = 0; ci < edge.parent_cols.size(); ci++) {
						if (ci > 0) {
							label += ", ";
						}
						label += table_mgr.GetColumnName(edge.parent_table, edge.parent_cols[ci].column_index) +
						         " / " +
						         table_mgr.GetColumnName(edge.child_table, edge.child_cols[ci].column_index);
					}
					info.edge_labels.push_back(label);
				}
			}

			if (!info.parent_centers.empty()) {
				conns.push_back(info);
			}
		}

		if (conns.empty()) {
			continue;
		}

		// determine connector width
		int conn_width = total_width;
		for (auto &info : conns) {
			for (int pc : info.parent_centers) {
				conn_width = std::max(conn_width, pc + 1);
			}
			conn_width = std::max(conn_width, info.child_center + 1);
			for (auto &lbl : info.edge_labels) {
				conn_width = std::max(conn_width, (int)lbl.size() + 2);
			}
		}

		// vertical lines from parents
		string vert1(conn_width, ' ');
		for (auto &info : conns) {
			for (int pc : info.parent_centers) {
				if (pc >= 0 && pc < conn_width) {
					vert1[pc] = '|';
				}
			}
		}
		output.push_back(vert1);

		// branch line (when parent and child not aligned, or multiple parents)
		string branch(conn_width, ' ');
		bool need_branch = false;

		for (auto &info : conns) {
			vector<int> all_pos = info.parent_centers;
			all_pos.push_back(info.child_center);
			int leftmost = *std::min_element(all_pos.begin(), all_pos.end());
			int rightmost = *std::max_element(all_pos.begin(), all_pos.end());

			if (leftmost != rightmost) {
				need_branch = true;
				for (int c = leftmost; c <= rightmost && c < conn_width; c++) {
					branch[c] = '-';
				}
				for (int pc : info.parent_centers) {
					if (pc >= 0 && pc < conn_width) {
						branch[pc] = '+';
					}
				}
				if (info.child_center >= 0 && info.child_center < conn_width) {
					branch[info.child_center] = '+';
				}
			}
		}
		if (need_branch) {
			output.push_back(branch);
		}

		for (auto &info : conns) {
			for (idx_t i = 0; i < info.edge_labels.size(); i++) {
				string &lbl = info.edge_labels[i];
				if (lbl.empty()) {
					continue;
				}
				string label_line(conn_width, ' ');
				int center = info.child_center;
				int start = center - (int)lbl.size() / 2;
				if (start < 0) {
					start = 0;
				}
				for (idx_t j = 0; j < lbl.size() && start + (int)j < conn_width; j++) {
					label_line[start + j] = lbl[j];
				}
				output.push_back(label_line);
			}
		}

		// vertical lines to children
		string vert2(conn_width, ' ');
		for (auto &info : conns) {
			if (info.child_center >= 0 && info.child_center < conn_width) {
				vert2[info.child_center] = '|';
			}
		}
		output.push_back(vert2);
	}

	// print
	Printer::Print("\n=== Physical Plan DAG ===");
	for (auto &line : output) {
		Printer::Print(line);
	}
	Printer::Print("=== Physical Plan DAG ===\n");
}

} // namespace duckdb
