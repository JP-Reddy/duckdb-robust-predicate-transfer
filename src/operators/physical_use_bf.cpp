#include "physical_use_bf.hpp"
#include "physical_create_bf.hpp"
#include "bloom_filter.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {

PhysicalUseBF::PhysicalUseBF(shared_ptr<BloomFilterOperation> bf_operation, vector<LogicalType> types,
                             idx_t estimated_cardinality, vector<idx_t> bound_column_indices)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      bf_operation(std::move(bf_operation)), bound_column_indices(std::move(bound_column_indices)) {
}

string PhysicalUseBF::GetName() const {
	return "USE_BF";
}

string PhysicalUseBF::ToString(ExplainFormat format) const {
	string result = "USE_BF";
	if (bf_operation) {
		result += " [" + std::to_string(bf_operation->probe_columns.size()) + " probe columns]";
	}
	return result;
}

unique_ptr<OperatorState> PhysicalUseBF::GetOperatorState(ExecutionContext &context) const {
	return make_uniq<PhysicalUseBFState>();
}

OperatorResultType PhysicalUseBF::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                         GlobalOperatorState &gstate, OperatorState &state_p) const {
	auto &bf_state = state_p.Cast<PhysicalUseBFState>();

	// lazy initialization of bloom filters on first call
	if (!bf_state.bloom_filters_initialized) {
		if (!related_create_bf_vec.empty()) {
			// get bloom filters from all related CREATE_BF operators
			for (auto *create_bf : related_create_bf_vec) {
				auto filters = create_bf->GetBloomFilters();
				bf_state.bloom_filters.insert(bf_state.bloom_filters.end(), filters.begin(), filters.end());
			}
		}
		bf_state.bloom_filters_initialized = true;
	}

	idx_t row_num = input.size();

	// if no bloom filters or no input, just pass through
	if (bf_state.bloom_filters.empty() || row_num == 0) {
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// apply bloom filters
	idx_t result_count = row_num;
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	for (auto &bf : bf_state.bloom_filters) {
		if (!bf || !bf->finalized_) {
			continue;
		}

		// check if bloom filter is empty (no data inserted)
		if (bf->num_sectors == 0 || bf->blocks == nullptr) {
			// empty filter means no matches possible
			chunk.SetCardinality(0);
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// use bound column indices for vectorized lookup
		vector<uint32_t> results(row_num);
		bf->Lookup(input, results, bound_column_indices);

		// build selection vector from results
		result_count = 0;
		for (idx_t i = 0; i < row_num; i++) {
			if (results[i] != 0) {
				sel.set_index(result_count++, i);
			}
		}

		// early exit if no rows passed
		if (result_count == 0) {
			chunk.SetCardinality(0);
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// apply filter if we filtered rows
		if (result_count < row_num) {
			input.Slice(sel, result_count);
			row_num = result_count;
		}
	}

	// optimization: if all rows passed, just reference input (zero-copy)
	if (result_count == row_num) {
		chunk.Reference(input);
	} else {
		chunk.Slice(input, sel, result_count);
	}

	return OperatorResultType::NEED_MORE_INPUT;
}

void PhysicalUseBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	string probe_table = bf_operation ? "table_" + std::to_string(bf_operation->probe_table_idx) : "unknown";
	Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s) BuildPipelines called", probe_table.c_str()));

	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, *this);
	Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s) added to current pipeline as operator", probe_table.c_str()));

	// add dependencies on all related CREATE_BF operators
	Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s) has %zu related CREATE_BF operators",
		probe_table.c_str(), related_create_bf_vec.size()));

	for (size_t i = 0; i < related_create_bf_vec.size(); i++) {
		auto *create_bf = related_create_bf_vec[i];
		string build_table = create_bf->bf_operation ?
			"table_" + std::to_string(create_bf->bf_operation->build_table_idx) : "unknown";
		Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s) adding dependency #%zu on CREATE_BF (build=%s)",
			probe_table.c_str(), i, build_table.c_str()));
		create_bf->BuildPipelinesFromRelated(current, meta_pipeline);

		Printer::Print(StringUtil::Format("[PIPELINE DEBUG] USE_BF (probe=%s) After adding dependency #%zu:", probe_table.c_str(), i));
		current.PrintDependencies();
	}

	Printer::Print(StringUtil::Format("[PIPELINE DEBUG] USE_BF (probe=%s) Final pipeline state:", probe_table.c_str()));
	try {
		current.Print();
		Printer::Print("Pipeline Dependencies");
		current.PrintDependencies();
	} catch (...) {
		Printer::Print("  (Pipeline not yet fully initialized)");
	}
	Printer::Print("");

	Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s) building child operator pipelines", probe_table.c_str()));

	// continue building child pipelines
	children[0].get().BuildPipelines(current, meta_pipeline);
}

} // namespace duckdb