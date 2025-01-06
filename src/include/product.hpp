#pragma once

#include "duckdb.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include <google/cloud/bigtable/table.h>

namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

unique_ptr<FunctionData> ProductFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names);

unique_ptr<GlobalTableFunctionState> ProductInitGlobal(ClientContext &context, TableFunctionInitInput &input);

unique_ptr<LocalTableFunctionState> ProductInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state);

void ProductFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output);
double ProductScanProgress(ClientContext &context, const FunctionData *bind_data,
                           const GlobalTableFunctionState *global_state);

static cbt::Filter make_filter(const vector<column_t> &column_ids);
static void ApplyFilter(const TableFilter &filter);
static void ApplyConstant(const ConstantFilter &filter);
} // namespace duckdb
