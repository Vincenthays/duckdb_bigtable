#pragma once

#include "duckdb.hpp"

namespace duckdb {

DUCKDB_EXTENSION_API unique_ptr<GlobalTableFunctionState> SearchInitGlobal(ClientContext &context, TableFunctionInitInput &input);
DUCKDB_EXTENSION_API unique_ptr<FunctionData> SearchFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                    vector<LogicalType> &return_types, vector<string> &names);
DUCKDB_EXTENSION_API void SearchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
