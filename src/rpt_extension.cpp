#define DUCKDB_EXTENSION_MAIN

#include "rpt_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <duckdb/parser/parsed_data/create_scalar_function_info.hpp>

// OpenSSL linked through vcpkg
#include <openssl/opensslv.h>

namespace duckdb {

inline void RptScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Rpt " + name.GetString() + " 🐥");
	});
}

inline void RptOpenSSLVersionScalarFun(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &name_vector = args.data[0];
	UnaryExecutor::Execute<string_t, string_t>(name_vector, result, args.size(), [&](string_t name) {
		return StringVector::AddString(result, "Rpt " + name.GetString() + ", my linked OpenSSL version is " +
		                                           OPENSSL_VERSION_TEXT);
	});
}

static void LoadInternal(DatabaseInstance &instance) {
	// Register a scalar function
	auto rpt_scalar_function = ScalarFunction("rpt", {LogicalType::VARCHAR}, LogicalType::VARCHAR, RptScalarFun);
	ExtensionUtil::RegisterFunction(instance, rpt_scalar_function);

	// Register another scalar function
	auto rpt_openssl_version_scalar_function = ScalarFunction("rpt_openssl_version", {LogicalType::VARCHAR},
	                                                            LogicalType::VARCHAR, RptOpenSSLVersionScalarFun);
	ExtensionUtil::RegisterFunction(instance, rpt_openssl_version_scalar_function);
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
