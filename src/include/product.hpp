#pragma once

#include "duckdb.hpp"

namespace duckdb {

DUCKDB_EXTENSION_API unique_ptr<GlobalTableFunctionState> ProductInitGlobal(ClientContext &context,
                                                                            TableFunctionInitInput &input);
DUCKDB_EXTENSION_API unique_ptr<FunctionData> ProductFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names);
DUCKDB_EXTENSION_API void ProductFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
