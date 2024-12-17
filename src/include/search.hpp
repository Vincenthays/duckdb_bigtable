#pragma once

#include "duckdb.hpp"

namespace duckdb {

unique_ptr<GlobalTableFunctionState> SearchInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<FunctionData> SearchFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names);
void SearchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

} // namespace duckdb
