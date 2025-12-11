#define DUCKDB_EXTENSION_MAIN

#include "product.hpp"
#include "search.hpp"
#include "bigtable2_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	TableFunction product("product",
	                      {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::BIGINT)},
	                      ProductFunction, ProductFunctionBind, ProductInitGlobal, ProductInitLocal);
	product.projection_pushdown = true;
	product.table_scan_progress = ProductScanProgress;
	loader.RegisterFunction(product);

	TableFunction search("search",
	                     {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER)},
	                     SearchFunction, SearchFunctionBind, SearchInitGlobal, SearchInitLocal);
	search.projection_pushdown = true;
	search.table_scan_progress = SearchScanProgress;
	loader.RegisterFunction(search);
}

void Bigtable2Extension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}

std::string Bigtable2Extension::Name() {
	return "bigtable2";
}

std::string Bigtable2Extension::Version() const {
#ifdef EXT_VERSION_BIGTABLE2
	return EXT_VERSION_BIGTABLE2;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(bigtable2, loader) {
	duckdb::LoadInternal(loader);
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
