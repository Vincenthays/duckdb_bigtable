#define DUCKDB_EXTENSION_MAIN

#include "product.hpp"
#include "search.hpp"
#include "bigtable2_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

static void LoadInternal(DatabaseInstance &db) {
	TableFunction product("product",
	                      {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::BIGINT)},
	                      ProductFunction, ProductFunctionBind, ProductInitGlobal);
	product.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(db, product);

	TableFunction search("search",
	                     {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::INTEGER)},
	                     SearchFunction, SearchFunctionBind, SearchInitGlobal);
	search.projection_pushdown = true;
	ExtensionUtil::RegisterFunction(db, search);
}

void Bigtable2Extension::Load(DuckDB &db) {
	LoadInternal(*db.instance);
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

DUCKDB_EXTENSION_API void bigtable2_init(duckdb::DatabaseInstance &db) {
	LoadInternal(db);
}

DUCKDB_EXTENSION_API const char *bigtable2_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
