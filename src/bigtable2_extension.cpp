#define DUCKDB_EXTENSION_MAIN

#include "bigtable2_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

namespace duckdb {

static unique_ptr<FunctionData> Bigtable2FunctionBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    return_types.emplace_back(LogicalType::UINTEGER);
    names.emplace_back("pe_id");
    return make_uniq<TableFunctionData>();
}

struct Bigtable2FunctionData : TableFunctionData {
    idx_t row_idx = 0;
};

void Bigtable2Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = (Bigtable2FunctionData &)*data.bind_data;

    if (state.row_idx >= 100) {
        output.SetCardinality(0);
        return;
    }

    output.SetValue(0, 0, Value::INTEGER(2));
    output.SetCardinality(1);

    state.row_idx++;
}

void Bigtable2Extension::Load(DuckDB &db) {
    TableFunction bigtable_function("bigtable2", {LogicalType::VARCHAR}, Bigtable2Function, Bigtable2FunctionBind);
    ExtensionUtil::RegisterFunction(*db.instance, bigtable_function);    
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
    duckdb::DuckDB db_wrapper(db);
    db_wrapper.LoadExtension<duckdb::Bigtable2Extension>();
}

DUCKDB_EXTENSION_API const char *bigtable2_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
