#include "rpt_pipeline.hpp"

#include "duckdb/parallel/pipeline.hpp"

#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/printer.hpp"
#include "duckdb/common/tree_renderer/text_tree_renderer.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/execution/operator/aggregate/physical_ungrouped_aggregate.hpp"
#include "duckdb/execution/operator/scan/physical_table_scan.hpp"
#include "duckdb/execution/operator/set/physical_recursive_cte.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/pipeline_event.hpp"
#include "duckdb/parallel/pipeline_executor.hpp"
#include "duckdb/parallel/task_scheduler.hpp"
#include "physical_create_bf.hpp"
#include "duckdb/execution/operator/join/physical_hash_join.hpp"

namespace duckdb {
	RptPipeline::Pipeline(Executor &executor_p)
	: executor(executor_p), num_source_chunks(0), num_source_rows(0), ready(false), initialized(false), source(nullptr),
	  sink(nullptr), is_selectivity_checked(false) {
	}

void RptPipeline::Schedule(shared_ptr<Event> &event) {
		D_ASSERT(ready);
		D_ASSERT(sink);

		// Dynamically change the operators of pipelines.
		ModifyCreateBFPipeline();
		Reset();
		if (!ScheduleParallel(event)) {
			// could not parallelize this pipeline: push a sequential task instead
			ScheduleSequentialTask(event);
		}
	}

void Pipeline::ModifyCreateBFPipeline() {
		if (source->type != PhysicalOperatorType::CREATE_BF) {
			return;
		}

		auto &bf_creator = source->Cast<PhysicalCreateBF>();
		if (bf_creator.is_successful) {
			return;
		}

		vector<reference<PhysicalOperator>> new_operators;
		PhysicalOperator *op = &bf_creator.children[0].get();
		while (true) {
			switch (op->type) {
			case PhysicalOperatorType::EXTENSION:
			case PhysicalOperatorType::FILTER:
			case PhysicalOperatorType::PROJECTION: {
				new_operators.push_back(*op);
				break;
			}
			case PhysicalOperatorType::EXTENSION: {
				auto &creator = op->Cast<PhysicalCreateBF>();
				if (!creator.is_successful) {
					break;
				}

				source = op;
				operators.insert(operators.begin(), new_operators.rbegin(), new_operators.rend());
				return;
			}
			case PhysicalOperatorType::EXPRESSION_SCAN:
			case PhysicalOperatorType::EMPTY_RESULT:
			case PhysicalOperatorType::DUMMY_SCAN:
			case PhysicalOperatorType::HASH_GROUP_BY:
			case PhysicalOperatorType::WINDOW:
			case PhysicalOperatorType::COLUMN_DATA_SCAN:
			case PhysicalOperatorType::CHUNK_SCAN:
			case PhysicalOperatorType::TABLE_SCAN:
			case PhysicalOperatorType::DELIM_SCAN: {
				source = op;
				operators.insert(operators.begin(), new_operators.rbegin(), new_operators.rend());
				return;
			}
			default:
				throw InternalException("Unknown operator type " + PhysicalOperatorToString(op->type) + "\n");
			}

			op = &op->children[0].get();
		}
	}

}