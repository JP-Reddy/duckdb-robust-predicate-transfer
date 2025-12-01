#include "physical_use_bf.hpp"
#include "physical_create_bf.hpp"
#include "dag.hpp"
#include "bloom_filter.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/selection_vector.hpp"
#include "duckdb/parallel/meta_pipeline.hpp"

namespace duckdb {

PhysicalUseBF::PhysicalUseBF(shared_ptr<FilterPlan> filter_plan, vector<LogicalType> types, idx_t estimated_cardinality)
    : PhysicalOperator(PhysicalOperatorType::EXTENSION, std::move(types), estimated_cardinality),
      filter_plan(std::move(filter_plan)), is_probing_side(false) {
}

string PhysicalUseBF::GetName() const {
    return "USE_BF";
}

string PhysicalUseBF::ToString(ExplainFormat format) const {
    string result = "USE_BF";
    if (filter_plan) {
        result += " [" + std::to_string(filter_plan->apply.size()) + " expressions]";
    }
    return result;
}

unique_ptr<OperatorState> PhysicalUseBF::GetOperatorState(ExecutionContext &context) const {
    return make_uniq<PhysicalUseBFState>();
}

unique_ptr<GlobalOperatorState> PhysicalUseBF::GetGlobalOperatorState(ClientContext &context) const {
    return make_uniq<PhysicalUseBFGlobalState>();
}

// void PhysicalUseBF::InitializeBloomFilters(PhysicalUseBFState &bf_state, ExecutionContext &context) const {
//     if (related_create_bf && !bf_state.bloom_filters_initialized) {
//         // get bloom filters from the related PhysicalCreateBF
//         bf_state.bloom_filters = related_create_bf->GetBloomFilters();
//         bf_state.bloom_filters_initialized = true;
//     }
// }

void PhysicalUseBF::InitializeBloomFilters(PhysicalUseBFState &bf_state, ExecutionContext &context) const {
	if (!bf_state.bloom_filters_initialized && !related_create_bf_vec.empty()) {
		// get bloom filters from the first related PhysicalCreateBF
		// (or merge from all of them if you have multiple)
		bf_state.bloom_filters = related_create_bf_vec[0]->GetBloomFilters();
		bf_state.bloom_filters_initialized = true;
	}
}

bool PhysicalUseBF::FilterDataChunk(DataChunk &chunk, const vector<shared_ptr<BloomFilter>> &bloom_filters,
                                    ExecutionContext &context) const {
    if (!filter_plan || filter_plan->apply.empty() || bloom_filters.empty()) {
    	// No filtering needed
    	return true;
    }
    
    // simplified version: use bound column indices directly from input chunk
    // note: this assumes the chunk already contains the correct columns
    
    // use bloom filters to filter rows using bound columns
    bool any_rows_remain = false;
    SelectionVector sel(chunk.size());
    idx_t result_count = 0;
    
    for (idx_t i = 0; i < chunk.size(); i++) {
        bool passes_filter = true;
        
        // check against all available bloom filters
        for (auto &bf : bloom_filters) {
            if (bf && bf->finalized_) {
                vector<uint32_t> results(1);
                
                // create single-row chunk for lookup using input chunk columns
                DataChunk single_row_chunk;
                single_row_chunk.Initialize(Allocator::DefaultAllocator(), chunk.GetTypes());
                
                // copy row data
                for (idx_t col = 0; col < chunk.ColumnCount(); col++) {
                    single_row_chunk.data[col].Slice(chunk.data[col], i, i + 1);
                }
                single_row_chunk.SetCardinality(1);
                
                // determine which columns to use for lookup
                vector<idx_t> lookup_cols;
                if (!filter_plan->bound_cols_apply.empty()) {
                    lookup_cols = filter_plan->bound_cols_apply;
                } else {
                    // use all columns if no specific binding
                    for (idx_t k = 0; k < single_row_chunk.ColumnCount(); k++) {
                        lookup_cols.push_back(k);
                    }
                }
                
                // Perform bloom filter lookup
                bf->Lookup(single_row_chunk, results, lookup_cols);
                
                if (results[0] == 0) {
                    passes_filter = false;
                    break; // Row doesn't pass bloom filter
                }
            }
        }
        
        if (passes_filter) {
            sel.set_index(result_count++, i);
            any_rows_remain = true;
        }
    }
    
    // apply selection to the chunk if we filtered any rows
    if (result_count < chunk.size()) {
        chunk.Slice(sel, result_count);
    }
    
    return any_rows_remain;
}

OperatorResultType PhysicalUseBF::Execute(ExecutionContext &context, DataChunk &input, DataChunk &chunk,
                                         GlobalOperatorState &gstate, OperatorState &state) const {

	Printer::Print("PhysicalUseBF::Execute() called");
    auto &bf_state = state.Cast<PhysicalUseBFState>();
    auto &bf_gstate = gstate.Cast<PhysicalUseBFGlobalState>();
    
    // initialize bloom filters if not done yet
    if (!bf_state.bloom_filters_initialized) {
        InitializeBloomFilters(bf_state, context);
    }
    
    // get data from child operator
    auto child_result = OperatorResultType::NEED_MORE_INPUT;
    if (!children.empty()) {
        child_result = children[0].get().Execute(context, input, chunk, gstate, state);
    } else {
        chunk.Reference(input);
        child_result = OperatorResultType::HAVE_MORE_OUTPUT;
    }
    
    // apply bloom filter if we have data and bloom filters are available
    if (chunk.size() > 0 && !bf_state.bloom_filters.empty()) {
        bool has_data = FilterDataChunk(chunk, bf_state.bloom_filters, context);
        
        // if all rows were filtered out, continue with empty chunk
        if (!has_data && chunk.size() > 0) {
            chunk.Reset();
        }
    }
    
    return child_result;
}

void PhysicalUseBF::BuildPipelines(Pipeline &current, MetaPipeline &meta_pipeline) {
	op_state.reset();

	auto &state = meta_pipeline.GetState();
	state.AddPipelineOperator(current, *this);

	// for each related CREATE_BF, ensure its pipeline is a dependency
	for (auto *create_bf : related_create_bf_vec) {
		create_bf->BuildPipelinesFromRelated(current, meta_pipeline);
	}

	// continue building child pipelines
	children[0].get().BuildPipelines(current, meta_pipeline);
}

} // namespace duckdb