#define DUCKDB_EXTENSION_MAIN

#include "rpt_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/scalar_function.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator_extension.hpp"
#include "operators/logical_create_bf.hpp"
#include "operators/logical_use_bf.hpp"
#include "optimizer/rpt_optimizer.hpp"
#include "duckdb/main/config.hpp"

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

static void LoadInternal(ExtensionLoader &loader) {
	// Register the SIP optimizer rule
	OptimizerExtension optimizer;
	// optimizer.pre_optimize_function = RPTOptimizerContextState::PreOptimize;
	optimizer.optimize_function = RPTOptimizerContextState::Optimize;

	DatabaseInstance &instance = loader.GetDatabaseInstance();
	instance.config.optimizer_extensions.push_back(optimizer);

	// Register logical operators
	instance.config.operator_extensions.push_back(make_uniq<CreateBFOperatorExtension>());
	instance.config.operator_extensions.push_back(make_uniq<UseBFOperatorExtension>());

	// Register profiling setting
	auto &config = DBConfig::GetConfig(instance);
	config.AddExtensionOption("rpt_profiling", "Enable RPT extension profiling output",
	                          LogicalType::BOOLEAN, Value::BOOLEAN(false));
}

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

DUCKDB_CPP_EXTENSION_ENTRY(rpt, loader) {
	duckdb::LoadInternal(loader);
}

DUCKDB_EXTENSION_API const char *rpt_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
