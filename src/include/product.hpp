#pragma once

#include "duckdb.hpp"

namespace duckdb {

unique_ptr<GlobalTableFunctionState> ProductInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<FunctionData> ProductFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names);
void ProductFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
