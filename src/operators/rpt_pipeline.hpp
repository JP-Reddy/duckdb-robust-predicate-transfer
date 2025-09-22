#include "duckdb/parallel/pipeline.hpp"

namespace duckdb {

class RptPipeline : public Pipeline {
	public:
		explicit RptPipeline(Executor &execution_context);

	//! Runtime Statistics for Predicate Transfer
		std::atomic<int64_t> num_source_chunks;
		std::atomic<int64_t> num_source_rows;

		bool is_building_bf = false;

	public:
		void ModifyCreateBFPipeline();

	private:
		atomic<bool> is_selectivity_checked;
}