#define DUCKDB_EXTENSION_MAIN

#include "rpt_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator_extension.hpp"
// #include "operators/logical_hello.hpp"
// #include "operators/physical_hello.hpp"
#include "operators/logical_create_bf.hpp"
#include "operators/logical_use_bf.hpp"
// #include "predicate_transfer_optimization.hpp"
#include "optimizer/rpt_optimizer.hpp"

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

class CreateBFOperatorExtension : public OperatorExtension {
public:
	std::string GetName() override {
		return "logical_create_bf";
	}
	
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
		return make_uniq<LogicalCreateBF>();
	}
};

class UseBFOperatorExtension : public OperatorExtension {
public:
	std::string GetName() override {
		return "logical_use_bf";
	}
	
	unique_ptr<LogicalExtensionOperator> Deserialize(Deserializer &deserializer) override {
		return make_uniq<LogicalUseBF>();
	}
};

static void LoadInternal(DatabaseInstance &instance) {
	// Register the SIP optimizer rule
	OptimizerExtension optimizer;
	// optimizer.optimize_function = PredicateTransferOptimizer::Optimize;
	// optimizer.pre_optimize_function = PredicateTransferOptimizer::PreOptimize;
	// optimizer.pre_optimize_function = RPTOptimizerContextState::PreOptimize;
	optimizer.optimize_function = RPTOptimizerContextState::Optimize;
	// optimizer.pre_optimize_function = PredicateTransferOptimizer::PreOptimize;
	instance.config.optimizer_extensions.push_back(optimizer);

	// Register logical operators
	instance.config.operator_extensions.push_back(make_uniq<CreateBFOperatorExtension>());
	instance.config.operator_extensions.push_back(make_uniq<UseBFOperatorExtension>());
}

static void LoadInternal(ExtensionLoader &loader) {
	// Register the SIP optimizer rule
	OptimizerExtension optimizer;
	// optimizer.optimize_function = PredicateTransferOptimizer::Optimize;
	// optimizer.pre_optimize_function = PredicateTransferOptimizer::PreOptimize;
	// optimizer.pre_optimize_function = RPTOptimizerContextState::PreOptimize;
	optimizer.optimize_function = RPTOptimizerContextState::Optimize;
	// optimizer.pre_optimize_function = PredicateTransferOptimizer::PreOptimize;

	DatabaseInstance &instance = loader.GetDatabaseInstance();
	instance.config.optimizer_extensions.push_back(optimizer);

	// Register logical operators
	instance.config.operator_extensions.push_back(make_uniq<CreateBFOperatorExtension>());
	instance.config.operator_extensions.push_back(make_uniq<UseBFOperatorExtension>());
}

// void RptExtension::Load(DuckDB &db) {
// 	LoadInternal(*db.instance);
// }
std::string RptExtension::Name() {
	return "rpt";
}

std::string RptExtension::Version() const {
#ifdef EXT_VERSION_RPT
	return EXT_VERSION_RPT;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

// DUCKDB_EXTENSION_API void rpt_init(duckdb::DatabaseInstance &db) {
// 	duckdb::DuckDB db_wrapper(db);
// 	db_wrapper.LoadExtension<duckdb::RptExtension>();
// }

DUCKDB_CPP_EXTENSION_ENTRY(vss, loader) {
	duckdb::LoadInternal(loader);
}

DUCKDB_EXTENSION_API const char *rpt_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
