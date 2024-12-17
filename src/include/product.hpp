#pragma once

#include "duckdb.hpp"

namespace duckdb {

unique_ptr<FunctionData> ProductFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<GlobalTableFunctionState> ProductInitGlobal(ClientContext &context, TableFunctionInitInput &input);
void ProductFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
