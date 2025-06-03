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

double SearchScanProgress(ClientContext &context, const FunctionData *bind_data,
                          const GlobalTableFunctionState *global_state);
} // namespace duckdb
