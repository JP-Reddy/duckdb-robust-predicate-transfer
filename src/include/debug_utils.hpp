//===----------------------------------------------------------------------===//
//                         RPT Extension
//
// debug_utils.hpp
//
// Debug printing utilities - prints only in debug builds
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/printer.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

// debug print macro - only prints in debug builds, no-op in release
#ifdef DEBUG

#define D_PRINT(...)  Printer::Print(__VA_ARGS__)
#define D_PRINTF(...) Printer::PrintF(__VA_ARGS__)

#else

#define D_PRINT(...)  ((void)0)
#define D_PRINTF(...) ((void)0)

#endif

} // namespace duckdb
