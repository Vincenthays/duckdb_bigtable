#define DUCKDB_EXTENSION_MAIN

#include "bigtable2_extension.hpp"
#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <google/cloud/bigtable/table.h>

using google::cloud::bigtable::Table;
using google::cloud::bigtable::MakeDataClient;
using google::cloud::bigtable::Filter;
using ::google::cloud::StatusOr;

namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

struct Bigtable2FunctionData : TableFunctionData {
    idx_t row_idx = 0;
    idx_t prefix_idx = 0;
    idx_t prefix_count;
    vector<string> prefixes;
    shared_ptr<Table> table;
};

static unique_ptr<FunctionData> Bigtable2FunctionBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("pe_id");
    return_types.emplace_back(LogicalType::UBIGINT);
    names.emplace_back("date");
    return_types.emplace_back(LogicalType::DATE);
    names.emplace_back("shop_id");
    return_types.emplace_back(LogicalType::UINTEGER);
    names.emplace_back("price");
    return_types.emplace_back(LogicalType::FLOAT);
    names.emplace_back("base_price");
    return_types.emplace_back(LogicalType::FLOAT);
    names.emplace_back("unit_price");
    return_types.emplace_back(LogicalType::FLOAT);
    names.emplace_back("promo_id");
    return_types.emplace_back(LogicalType::LIST(LogicalType::UINTEGER));
    names.emplace_back("promo_text");
    return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
    names.emplace_back("shelf");
    return_types.emplace_back(LogicalType::LIST(LogicalType::VARCHAR));
    names.emplace_back("position");
    return_types.emplace_back(LogicalType::LIST(LogicalType::UINTEGER));
    names.emplace_back("is_paid");
    return_types.emplace_back(LogicalType::LIST(LogicalType::BOOLEAN));

    auto bind_data = make_uniq<Bigtable2FunctionData>();

    auto data_client = MakeDataClient("dataimpact-processing", "processing");
    auto table = make_shared_ptr<Table>(data_client, "product");
    bind_data->table = table;
    
    auto prefixes = ListValue::GetChildren(input.inputs[0]);
    for (auto &p : prefixes) {
        string prefix = StringValue::Get(p);
        bind_data->prefixes.emplace_back(prefix);
    }
    bind_data->prefix_count = bind_data->prefixes.size();

    return std::move(bind_data);
}

void Bigtable2Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = (Bigtable2FunctionData &)*data.bind_data;

    if (state.prefix_idx == state.prefix_count) {
        output.SetCardinality(0);
        return;
    }

    idx_t cardinality = 0;

    for (StatusOr<cbt::Row>& row : state.table->ReadRows(
        cbt::RowRange::Prefix(state.prefixes[state.prefix_idx++]),
        cbt::Filter::PassAllFilter()
    )) {
        if (!row) throw std::move(row).status();
        
        auto row_key = row.value().row_key();
        auto index_1 = row_key.find_first_of('/');
        auto index_2 =  row_key.find_last_of('/');

        uint64_t pe_id = std::stoul(row_key.substr(0, index_1));
        string_t date = row_key.substr(index_1 + 1, index_2 - index_1 - 1);
        uint32_t shop_id = std::stoul(row_key.substr(index_2 + 1));

        bool arr_mask[7];
        Value arr_date[7];
        Value arr_price[7];
        Value arr_base_price[7];
        Value arr_unit_price[7];
        vector<Value> arr_promo_id[7];
        vector<Value> arr_promo_text[7];
        vector<Value> arr_shelf[7];
        vector<Value> arr_position[7];
        vector<Value> arr_is_paid[7];
        
        for (auto& cell : row.value().cells()) {
            
            date_t date = Date::EpochToDate(cell.timestamp().count() / 1000000);
            int weekday = Date::ExtractISODayOfTheWeek(date) - 1;
            
            arr_mask[weekday] = true;
            arr_date[weekday] = Value::DATE(date);

            switch (cell.family_name().at(0)) {
            case 'p':
                switch (cell.column_qualifier().at(0)) {
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
            case 'd':
                arr_promo_id[weekday].emplace_back(Value(std::stoi(cell.column_qualifier())));
                arr_promo_text[weekday].emplace_back(Value(cell.value()));
                break;
            case 's' | 'S':
                arr_shelf[weekday].emplace_back(cell.column_qualifier());
                arr_position[weekday].emplace_back(Value(std::stoi(cell.value())));
                arr_is_paid[weekday].emplace_back(Value(cell.column_qualifier().at(0) == 'S'));
                break;
            }
        }

        for (int i = 0; i < 7; i++) {
            if (!arr_mask[i]) {
                continue;
            }
            output.SetValue(0, state.row_idx, Value::UBIGINT(pe_id));
            output.SetValue(1, state.row_idx, arr_date[i]);
            output.SetValue(2, state.row_idx, Value::UINTEGER(shop_id));
            output.SetValue(3, state.row_idx, arr_price[i]);
            output.SetValue(4, state.row_idx, arr_base_price[i]);
            output.SetValue(5, state.row_idx, arr_unit_price[i]);
            output.SetValue(6, state.row_idx, Value::LIST(LogicalType::UINTEGER, arr_promo_id[i]));
            output.SetValue(7, state.row_idx, Value::LIST(LogicalType::VARCHAR, arr_promo_text[i]));
            output.SetValue(8, state.row_idx, Value::LIST(LogicalType::VARCHAR, arr_shelf[i]));
            output.SetValue(9, state.row_idx, Value::LIST(LogicalType::UINTEGER, arr_position[i]));
            output.SetValue(10, state.row_idx, Value::LIST(LogicalType::BOOLEAN, arr_is_paid[i]));

            cardinality++;
            state.row_idx++;
        }
    }
    
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
