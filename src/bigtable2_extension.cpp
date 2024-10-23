#define DUCKDB_EXTENSION_MAIN

#include "bigtable2_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <google/cloud/bigtable/table.h>

using ::google::cloud::StatusOr;
using google::cloud::bigtable::Filter;
using google::cloud::bigtable::MakeDataClient;
using google::cloud::bigtable::RowReader;
using google::cloud::bigtable::Table;

namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

struct Bigtable2FunctionData : TableFunctionData {
  idx_t row_idx = 0;
  idx_t prefix_idx = 0;
  idx_t prefix_count;
  vector<string> prefixes_start;
  vector<string> prefixes_end;

  shared_ptr<Table> table;
};

static unique_ptr<FunctionData> Bigtable2FunctionBind(
  ClientContext &context, 
  TableFunctionBindInput &input,
  vector<LogicalType> &return_types,
  vector<string> &names
) {
  // Define output column names and types
  names = {"pe_id", "date", "shop_id", "price", "base_price", "unit_price", 
           "promo_id", "promo_text", "shelf", "position", "is_paid"};

  return_types = {LogicalType::UBIGINT, LogicalType::DATE, LogicalType::UINTEGER, 
                  LogicalType::FLOAT, LogicalType::FLOAT, LogicalType::FLOAT,
                  LogicalType::UINTEGER, LogicalType::VARCHAR, 
                  LogicalType::LIST(LogicalType::VARCHAR),
                  LogicalType::LIST(LogicalType::UINTEGER),
                  LogicalType::LIST(LogicalType::BOOLEAN)};

  auto bind_data = make_uniq<Bigtable2FunctionData>();

  auto data_client = MakeDataClient("dataimpact-processing", "processing");
  bind_data->table = make_shared_ptr<Table>(data_client, "product");

  auto ls_pe_id = ListValue::GetChildren(input.inputs[0]);
  bind_data->prefix_count = ls_pe_id.size();

  for (const auto &pe_id : ls_pe_id) {
    string prefix_id = StringValue::Get(pe_id);
    reverse(prefix_id.begin(), prefix_id.end());
    bind_data->prefixes_start.emplace_back(prefix_id + "/202424/");
    bind_data->prefixes_end.emplace_back(prefix_id + "/2024240");
  }

  return std::move(bind_data);
}

void Bigtable2Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
  auto &state = (Bigtable2FunctionData &)*data.bind_data;

  idx_t cardinality = 0;

  // Check if all prefixes have been processed
  if (state.prefix_idx >= state.prefix_count) {
    output.SetCardinality(0);
    return;
  }

  // Define range and filter for Bigtable query
  auto range = cbt::RowRange::Range(
    state.prefixes_start[state.prefix_idx], 
    state.prefixes_end[state.prefix_idx]
  );
  auto filter = Filter::PassAllFilter();
  state.prefix_idx++;  // Move to next prefix for next invocation

  // Process each row in the result set
  for (StatusOr<cbt::Row> &row_result : state.table->ReadRows(range, filter)) {
    if (!row_result) {
      throw std::runtime_error(row_result.status().message());
    }

    const auto &row = row_result.value();
    const auto &row_key = row.row_key();
    auto index_1 = row_key.find_first_of('/');
    auto index_2 = row_key.find_last_of('/');

    // Extract and reverse prefix_id, parse pe_id and shop_id
    string prefix_id = row_key.substr(0, index_1);
    reverse(prefix_id.begin(), prefix_id.end());
    uint64_t pe_id = std::stoull(prefix_id);
    uint32_t shop_id = std::stoul(row_key.substr(index_2 + 1));

    // Arrays to hold cell data for each day (7 days)
    std::array<bool, 7> arr_mask = {false};
    std::array<Value, 7> arr_date, arr_price, arr_base_price, arr_unit_price, arr_promo_id, arr_promo_text;
    std::array<vector<Value>, 7> arr_shelf, arr_position, arr_is_paid;

    // Iterate over each cell in the row
    for (const auto &cell : row.cells()) {
      // Convert timestamp to date and get weekday (0-based index for Mon-Sun)
      date_t date = Date::EpochToDate(cell.timestamp().count() / 1000000);
      int32_t weekday = Date::ExtractISODayOfTheWeek(date) - 1;
      arr_mask[weekday] = true;  // Mark day as having valid data
      arr_date[weekday] = Value::DATE(date);

      // Process data based on column family and qualifier
      switch (cell.family_name()[0]) {
        case 'p':  // Price-related data
          switch (cell.column_qualifier()[0]) {
            case 'p':
              arr_price[weekday] = Value(std::stod(cell.value()));
              break;
            case 'b':
              arr_base_price[weekday] = Value(std::stod(cell.value()));
              break;
            case 'u':
              arr_unit_price[weekday] = Value(std::stod(cell.value()));
              break;
          }
          break;

        case 'd':  // Promo-related data
          arr_promo_id[weekday] = Value::UINTEGER(std::stoul(cell.column_qualifier()));
          arr_promo_text[weekday] = Value(cell.value());  // Use Value() constructor for strings
          break;

        case 's':  // Shelf-related data (unpaid)
        case 'S':  // Shelf-related data (paid)
          arr_shelf[weekday].emplace_back(Value(cell.column_qualifier()));  // Use Value() constructor for strings
          arr_position[weekday].emplace_back(Value::UINTEGER(std::stoul(cell.value())));
          arr_is_paid[weekday].emplace_back(Value::BOOLEAN(cell.family_name()[0] == 'S'));
          break;
      }
    }

    // Set output values for each valid day (Monday-Sunday)
    for (int i = 0; i < 7; ++i) {
      if (!arr_mask[i]) continue;

      output.SetValue(0, state.row_idx, Value::UBIGINT(pe_id));
      output.SetValue(1, state.row_idx, arr_date[i]);
      output.SetValue(2, state.row_idx, Value::UINTEGER(shop_id));
      output.SetValue(3, state.row_idx, arr_price[i]);
      output.SetValue(4, state.row_idx, arr_base_price[i]);
      output.SetValue(5, state.row_idx, arr_unit_price[i]);
      output.SetValue(6, state.row_idx, arr_promo_id[i]);
      output.SetValue(7, state.row_idx, arr_promo_text[i]);

      // Set LIST values if available
      if (!arr_shelf[i].empty()) {
        output.SetValue(8, state.row_idx, Value::LIST(arr_shelf[i]));
        output.SetValue(9, state.row_idx, Value::LIST(arr_position[i]));
        output.SetValue(10, state.row_idx, Value::LIST(arr_is_paid[i]));
      }

      cardinality++;
      state.row_idx++;
    }
  }

  std::cout << "Processed prefix: " << state.prefixes_start[state.prefix_idx - 1]
            << " - " << state.row_idx << " rows processed." << std::endl;

  output.SetCardinality(cardinality);
}

void Bigtable2Extension::Load(DuckDB &db) {
  TableFunction bigtable_function("bigtable2", {LogicalType::LIST(LogicalType::VARCHAR)}, Bigtable2Function, Bigtable2FunctionBind);
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
