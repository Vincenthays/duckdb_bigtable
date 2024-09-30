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
    shared_ptr<Table> table;
};

static unique_ptr<FunctionData> Bigtable2FunctionBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types, vector<string> &names) {
    names.emplace_back("pe_id");
    return_types.emplace_back(LogicalType::UBIGINT);
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

    auto data_client = MakeDataClient("dataimpact-processing", "processing");
    auto table = make_shared_ptr<Table>(data_client, "product");

    auto bind_data = make_uniq<Bigtable2FunctionData>();
    bind_data->table = table;

    return std::move(bind_data);
}

void Bigtable2Function(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
    auto &state = (Bigtable2FunctionData &)*data.bind_data;
    idx_t cardinality = 0; 

    for (StatusOr<cbt::Row>& row : state.table->ReadRows(
        cbt::RowSet("30000000001/202231/38590", "30000000001/202231/38593"),
        cbt::Filter::PassAllFilter()
    )) {
        if (!row) throw std::move(row).status();

        vector<Value> promo_id;
        vector<Value> promo_text;
        vector<Value> shelf;
        vector<Value> position;
        vector<Value> is_paid;
        
        for (auto& cell : row.value().cells()) {
            std::cout 
                << cell.family_name()
                << ":"
                << cell.column_qualifier()
                << "="
                << cell.value()
                << std::endl;

            switch (cell.family_name().at(0)) {
            case 'p':
                switch (cell.column_qualifier().at(0)) {
                case 'p':
                    output.SetValue(2, state.row_idx, Value(std::stof(cell.value())));
                    break;
                case 'b':
                    output.SetValue(3, state.row_idx, Value(std::stof(cell.value())));
                    break;
                case 'u':
                    output.SetValue(4, state.row_idx, Value(std::stof(cell.value())));
                    break;
                }
                break;
            case 'd':
                promo_id.emplace_back(Value(std::stoi(cell.column_qualifier())));
                promo_text.emplace_back(Value(cell.value()));
                break;
            case 's' | 'S':
                shelf.emplace_back(cell.column_qualifier());
                position.emplace_back(Value(std::stoi(cell.value())));
                is_paid.emplace_back(Value(cell.column_qualifier().at(0) == 'S'));
                break;
            default:
                break;
            }
        }
        output.SetValue(5, state.row_idx, Value::LIST(LogicalType::UINTEGER, std::move(promo_id)));
        output.SetValue(6, state.row_idx, Value::LIST(LogicalType::VARCHAR, std::move(promo_text)));
        output.SetValue(7, state.row_idx, Value::LIST(LogicalType::VARCHAR, std::move(shelf)));
        output.SetValue(8, state.row_idx, Value::LIST(LogicalType::UINTEGER, std::move(position)));
        output.SetValue(9, state.row_idx, Value::LIST(LogicalType::BOOLEAN, std::move(is_paid)));

        cardinality++;
        state.row_idx++;
    }

    if (state.row_idx >= 100) {
        output.SetCardinality(0);
        return;
    }

    output.SetCardinality(cardinality);
}

void Bigtable2Extension::Load(DuckDB &db) {
    TableFunction bigtable_function("bigtable2", {LogicalType::LIST(LogicalType::INTEGER)}, Bigtable2Function, Bigtable2FunctionBind);
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
