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

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {


static void LoadInternal(DatabaseInstance &instance) {

	// RegisterLogicalCreateBFOperator(instance);
}

void RptExtension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
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

DUCKDB_EXTENSION_API void rpt_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::RptExtension>();
}

DUCKDB_EXTENSION_API const char *rpt_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
