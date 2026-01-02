#include "physical_use_bf.hpp"
#include "physical_create_bf.hpp"
#include "bloom_filter.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {

PhysicalUseBF::PhysicalUseBF(shared_ptr<BloomFilterOperation> bf_operation, vector<LogicalType> types,
                             idx_t estimated_cardinality, vector<idx_t> bound_column_indices)
    : CachingPhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
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

OperatorResultType PhysicalUseBF::ExecuteInternal(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                                  GlobalOperatorState &gstate, OperatorState &state_p) const {
	string table_name = bf_operation ? "table_" + std::to_string(bf_operation->probe_table_idx) : "unknown";
	// printf("[EXEC_INTERNAL] USE_BF (probe=%s, this=%p) called with %llu input rows\n", table_name.c_str(), (void*)this, input.size());

	auto &bf_state = state_p.Cast<PhysicalUseBFState>();

	// lazy initialization of bloom filters on first call
	if (!bf_state.bloom_filters_initialized) {
		printf("[EXEC_INTERNAL] USE_BF (probe=%s, this=%p) Initializing bloom filters, bound_column_indices.size()=%zu\n",
			table_name.c_str(), (void*)this, bound_column_indices.size());
		for (size_t i = 0; i < bound_column_indices.size(); i++) {
			printf("  bound_column_indices[%zu] = %llu\n", i, bound_column_indices[i]);
		}

		if (!related_create_bf_vec.empty() && bf_operation) {
			// lookup bloom filters by build column binding
			for (const auto &build_col : bf_operation->build_columns) {
				for (auto *create_bf : related_create_bf_vec) {
					auto bf = create_bf->GetBloomFilter(build_col);
					if (bf) {
						string build_table = create_bf->bf_operation ?
							"table_" + std::to_string(create_bf->bf_operation->build_table_idx) : "unknown";
						printf("[EXEC_INTERNAL] USE_BF found bloom filter for col(%llu,%llu) from CREATE_BF (build=%s)\n",
							build_col.table_index, build_col.column_index, build_table.c_str());
						bf_state.bloom_filters.push_back(bf);
						break; // found the filter for this column
					}
				}
			}
		}
		printf("[EXEC_INTERNAL] USE_BF total bloom_filters.size() = %zu\n", bf_state.bloom_filters.size());
		bf_state.bloom_filters_initialized = true;
	}

	idx_t row_num = input.size();

	// if no bloom filters or no input, just pass through
	if (bf_state.bloom_filters.empty() || row_num == 0) {
		printf("[EXEC_INTERNAL] USE_BF (probe=%s, this=%p) No bloom filter input/empty, row_num = %llu\n", table_name.c_str(), (void*)this, row_num);
		chunk.Reference(input);
		return OperatorResultType::NEED_MORE_INPUT;
	}

	// apply bloom filters
	idx_t result_count = row_num;
	SelectionVector sel(STANDARD_VECTOR_SIZE);

	for (int i = 0; i < bf_state.bloom_filters.size(); i++) {
		auto bf = bf_state.bloom_filters[i];
		if (!bf || !bf->finalized_) {
			printf("skipppppped");
			continue;
		}

		// check if bloom filter is empty (no data inserted)
		if (bf->num_sectors == 0 || bf->blocks == nullptr) {
			string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
			printf("Bloom filter empty for %s\n", build_table.c_str());
			// empty filter means no matches possible
			chunk.SetCardinality(0);
			return OperatorResultType::NEED_MORE_INPUT;
		}

		// string probe_table = bf_operation ? "table_" + std::to_string(bf_operation->probe_table_idx) : "unknown";
		// for (int i = 0; i < bound_column_indices.size(); i++) {
		// 	printf("bound columns for %s - %llu\n", probe_table.c_str(), bound_column_indices[i]);
		// }

		if (!bf_state.tested_hardcoded && bf_operation && bf_operation->build_table_idx == 3) {
			printf("\n[HARDCODED TEST IN USE_BF] Testing if 37 title IDs can be found in bloom filter from table_3...\n");

			vector<int32_t> test_ids = {
				929582, 1547687, 1669098, 1688430, 1695344, 1710439, 1779162, 1739896,
				1791810, 1715711, 1741821, 1847064, 1865784, 1808171, 1957879, 1931339,
				1961996, 1964522, 2053304, 2168207, 2183752, 2134798, 2166666, 2297104,
				2224135, 2209537, 2247126, 2384186, 2310404, 2455749, 2468515, 2421394,
				2434310, 2425149, 2468797, 2437383, 2518849
			};

			// Create test chunk
			DataChunk test_chunk;
			test_chunk.Initialize(Allocator::DefaultAllocator(), {LogicalType::INTEGER});
			test_chunk.SetCardinality(test_ids.size());

			auto test_data = FlatVector::GetData<int32_t>(test_chunk.data[0]);
			for (size_t i = 0; i < test_ids.size(); i++) {
				test_data[i] = test_ids[i];
			}

			// Test lookup with column index 0 (same as bound_column_indices should be)
			vector<uint32_t> test_results(test_ids.size());
			vector<idx_t> test_cols = {0};
			bf->Lookup(test_chunk, test_results, test_cols);

			int found = 0;
			int missing = 0;
			printf("  Testing %zu IDs using column index 0:\n", test_ids.size());
			for (size_t i = 0; i < test_ids.size(); i++) {
				if (test_results[i] != 0) {
					found++;
				} else {
					missing++;
					printf("    ❌ ID %d NOT FOUND (BUG!)\n", test_ids[i]);
				}
			}
			printf("  Found: %d / %zu\n", found, test_ids.size());
			printf("  Missing: %d / %zu\n", missing, test_ids.size());

			if (missing > 0) {
				printf("  ❌ BLOOM FILTER LOOKUP FAILED FROM USE_BF!\n");
			} else {
				printf("  ✅ All 37 IDs found in bloom filter from USE_BF!\n");
			}

			bf_state.tested_hardcoded = true;
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
			// printf("[EXEC_INTERNAL] USE_BF (probe=%s, this=%p) Result count = 0\n", table_name.c_str(), (void*)this);
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

	// char ptr_str[32];
	// snprintf(ptr_str, sizeof(ptr_str), "%p", (void*)this);
	// Printer::Print(StringUtil::Format("[PIPELINE] CREATE_BF (build=%s, this=%s) BuildPipelines called", build_table.c_str(), ptr_str));
	string probe_table = bf_operation ? "table_" + std::to_string(bf_operation->probe_table_idx) : "unknown";
	string build_table = bf_operation ? "table_" + std::to_string(bf_operation->build_table_idx) : "unknown";
	// printf("[EXECUTE] USE_BF (probe=%s, this=%p, build=%s) Selected %llu rows \n", probe_table.c_str(), (void*)this, build_table.c_str(), result_count);
	// chunk.Print();
	return OperatorResultType::NEED_MORE_INPUT;
}

void PhysicalUseBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	char ptr_str[32];
	snprintf(ptr_str, sizeof(ptr_str), "%p", (void*)this);
	// Printer::Print(StringUtil::Format("[PIPELINE] CREATE_BF (build=%s, this=%s) BuildPipelines called", build_table.c_str(), ptr_str));
	string probe_table = bf_operation ? "table_" + std::to_string(bf_operation->probe_table_idx) : "unknown";
	Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s, this=%s) BuildPipelines called", probe_table.c_str(), ptr_str));

	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, *this);
	Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s, this=%s) added to current pipeline as operator", probe_table.c_str(), ptr_str));

	// add dependencies on all related CREATE_BF operators
	Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s) has %zu related CREATE_BF operators",
		probe_table.c_str(), related_create_bf_vec.size()));

	for (size_t i = 0; i < related_create_bf_vec.size(); i++) {
		auto *create_bf = related_create_bf_vec[i];
		// string build_table = create_bf->bf_operation ?
		// 	"table_" + std::to_string(create_bf->bf_operation->build_table_idx) : "unknown";
		// Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s) adding dependency #%zu on CREATE_BF (build=%s)",
		// 	probe_table.c_str(), i, build_table.c_str()));
		create_bf->BuildPipelinesFromRelated(current, meta_pipeline);

		// Printer::Print(StringUtil::Format("[PIPELINE DEBUG] USE_BF (probe=%s) After adding dependency #%zu:", probe_table.c_str(), i));
		// current.PrintDependencies();
	}

	// Printer::Print(StringUtil::Format("[PIPELINE DEBUG] USE_BF (probe=%s) Final pipeline state:", probe_table.c_str()));
	try {
		// current.Print();
		// Printer::Print("Pipeline Dependencies");
		// current.PrintDependencies();
	} catch (...) {
		// Printer::Print("  (Pipeline not yet fully initialized)");
	}
	// Printer::Print("");

	// Printer::Print(StringUtil::Format("[PIPELINE] USE_BF (probe=%s) building child operator pipelines", probe_table.c_str()));

	// continue building child pipelines
	children[0].get().BuildPipelines(current, meta_pipeline);
}

} // namespace duckdb