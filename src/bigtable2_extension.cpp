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

struct DayData {
  bool valid = false;
  Value pe_id;
  Value shop_id;
  Value date;
  Value price;
  Value base_price;
  Value unit_price;
  Value promo_id;
  Value promo_text;
  vector<Value> shelves;
};

struct Bigtable2FunctionData : TableFunctionData {
  shared_ptr<Table> table;
  vector<cbt::RowRange> ranges;
  vector<DayData> remainder;
};

static unique_ptr<FunctionData> Bigtable2FunctionBind(
  ClientContext &context, 
  TableFunctionBindInput &input,
  vector<LogicalType> &return_types,
  vector<string> &names
) {
  names = {"pe_id", "shop_id", "date", "price", "base_price", "unit_price", 
           "promo_id", "promo_text", "shelves"};

  return_types = {LogicalType::UBIGINT,
                  LogicalType::UINTEGER,
                  LogicalType::DATE,
                  LogicalType::FLOAT,
                  LogicalType::FLOAT,
                  LogicalType::FLOAT,
                  LogicalType::UINTEGER,
                  LogicalType::VARCHAR,
                  LogicalType::LIST(LogicalType::STRUCT({
                    {"shelf", LogicalType::VARCHAR}, 
                    {"position", LogicalType::UINTEGER}, 
                    {"is_paid", LogicalType::BOOLEAN}}))};

  auto bind_data = make_uniq<Bigtable2FunctionData>();

  // Connect to Bigtable
  auto data_client = MakeDataClient("dataimpact-processing", "processing");
  bind_data->table = make_shared_ptr<Table>(data_client, "product");

  // Extract and process parameters
  const auto week_start = std::to_string(IntegerValue::Get(input.inputs[0]));
  const auto week_end = std::to_string(IntegerValue::Get(input.inputs[1]));
  
  const auto ls_pe_id = ListValue::GetChildren(input.inputs[2]);
  for (const auto &pe_id : ls_pe_id) {
    string prefix_id = std::to_string(BigIntValue::Get(pe_id));
    reverse(prefix_id.begin(), prefix_id.end());
    bind_data->ranges.emplace_back(cbt::RowRange::Closed(
      prefix_id + "/" + week_start + "/", 
      prefix_id + "/" + week_end + "0"
    ));
  }

  return std::move(bind_data);
}

void Bigtable2Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
  const auto filter = Filter::PassAllFilter();
  auto &state = (Bigtable2FunctionData &)*data.bind_data;

  // Process each range
  while (!state.ranges.empty()) {
    const auto range = state.ranges[0];
    state.ranges.erase(state.ranges.begin());

    // Process each row in the result set
    for (StatusOr<cbt::Row> &row_result : state.table->ReadRows(range, filter)) {
      if (!row_result) throw std::runtime_error(row_result.status().message());

      const auto &row = row_result.value();
      const auto &row_key = row.row_key();

      // Extract pe_id and shop_id from row key
      const auto index_1 = row_key.find_first_of('/');
      const auto index_2 = row_key.find_last_of('/');
      string prefix_id = row_key.substr(0, index_1);
      reverse(prefix_id.begin(), prefix_id.end());
      const auto pe_id = Value::UBIGINT(std::stoull(prefix_id));
      const auto shop_id = Value::UINTEGER(std::stoul(row_key.substr(index_2 + 1)));

      // Array to hold data for each day of the week
      std::array<DayData, 7> day_data;

      // Iterate over each cell in the row
      for (const auto &cell : row.cells()) {
        // Convert timestamp to date and get weekday index (0-based, Mon-Sun)
        const date_t date = Date::EpochToDate(cell.timestamp().count() / 1000000);
        const int32_t weekday = Date::ExtractISODayOfTheWeek(date) - 1;

        // Get reference to DayData for the current weekday
        auto &current_day = day_data[weekday];
        current_day.valid = true;
        current_day.pe_id = pe_id;
        current_day.shop_id = shop_id;
        current_day.date = Value::DATE(date);

        // Process data based on column family and qualifier
        switch (cell.family_name()[0]) {
          case 'p': // Price data
            switch (cell.column_qualifier()[0]) {
              case 'p': current_day.price = Value(std::stod(cell.value())); break;
              case 'b': current_day.base_price = Value(std::stod(cell.value())); break;
              case 'u': current_day.unit_price = Value(std::stod(cell.value())); break;
            }
            break;
          case 'd': // Promo data
            current_day.promo_id = Value::UINTEGER(std::stoul(cell.column_qualifier()));
            current_day.promo_text = Value(cell.value()); 
            break;
          case 's': // Shelf data (unpaid)
          case 'S': // Shelf data (paid)
            current_day.shelves.emplace_back(Value::STRUCT({
              {"shelf", Value(cell.column_qualifier())},
              {"position", Value::UINTEGER(std::stoul(cell.value()))},
              {"is_paid", Value::BOOLEAN(cell.family_name()[0] == 'S')}}));
            break;
        }
      }

      for (const auto &day : day_data) {
        if (day.valid) state.remainder.emplace_back(day);
      }
    }

    if (!state.remainder.empty()) break;
  }

  idx_t cardinality = 0;

  for (const auto &day : state.remainder) {
    output.SetValue(0, cardinality, day.pe_id);
    output.SetValue(1, cardinality, day.shop_id);
    output.SetValue(2, cardinality, day.date);
    output.SetValue(3, cardinality, day.price);
    output.SetValue(4, cardinality, day.base_price);
    output.SetValue(5, cardinality, day.unit_price);
    output.SetValue(6, cardinality, day.promo_id);
    output.SetValue(7, cardinality, day.promo_text);
    output.SetValue(8, cardinality, Value::LIST(day.shelves));

    cardinality++;
    if (cardinality == STANDARD_VECTOR_SIZE) {
      state.remainder.erase(state.remainder.begin(), state.remainder.begin() + cardinality);
      output.SetCardinality(cardinality);
      return;
    }
  }

  state.remainder.clear();
  output.SetCardinality(cardinality);
}

void Bigtable2Extension::Load(DuckDB &db) {
  TableFunction bigtable_function(
    "bigtable2", 
    {LogicalType::INTEGER, LogicalType::INTEGER, LogicalType::LIST(LogicalType::BIGINT)}, 
    Bigtable2Function, Bigtable2FunctionBind);
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
