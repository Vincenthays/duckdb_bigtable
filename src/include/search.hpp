#pragma once

#include "duckdb.hpp"
#include <google/cloud/bigtable/table.h>

namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

unique_ptr<FunctionData> SearchFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names);
unique_ptr<GlobalTableFunctionState> SearchInitGlobal(ClientContext &context, TableFunctionInitInput &input);
unique_ptr<LocalTableFunctionState> SearchInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                    GlobalTableFunctionState *global_state);
void SearchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);

cbt::Filter SearchFilter(const vector<column_t> &column_ids);
} // namespace duckdb
