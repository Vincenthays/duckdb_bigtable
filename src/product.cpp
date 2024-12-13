#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <google/cloud/bigtable/table.h>

using ::google::cloud::StatusOr;
namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

struct ProductGlobalState : GlobalTableFunctionState {
	shared_ptr<cbt::Table> table;
};

DUCKDB_EXTENSION_API unique_ptr<GlobalTableFunctionState> ProductInitGlobal(ClientContext &context,
                                                                            TableFunctionInitInput &input) {
	auto global_state = make_uniq<ProductGlobalState>();
	global_state->table = make_shared_ptr<cbt::Table>(
	    cbt::MakeDataConnection(), cbt::TableResource("dataimpact-processing", "processing", "product"));
	return std::move(global_state);
}

struct Product {
	bool valid = false;
	Value pe_id;
	Value shop_id;
	Value date;
	Value price;
	Value base_price;
	Value unit_price;
	Value promo_id;
	Value promo_text;
	vector<Value> shelf;
	vector<Value> position;
	vector<Value> is_paid;
};

struct ProductFunctionData : TableFunctionData {
	idx_t ranges_idx = 0;
	vector<cbt::RowRange> ranges;

	idx_t remainder_idx = 0;
	vector<Product> remainder;
};

DUCKDB_EXTENSION_API unique_ptr<FunctionData> ProductFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                                                  vector<LogicalType> &return_types,
                                                                  vector<string> &names) {
	names = {"pe_id",    "shop_id",    "date",  "price",    "base_price", "unit_price",
	         "promo_id", "promo_text", "shelf", "position", "is_paid"};

	return_types = {LogicalType::UBIGINT,
	                LogicalType::UINTEGER,
	                LogicalType::DATE,
	                LogicalType::FLOAT,
	                LogicalType::FLOAT,
	                LogicalType::FLOAT,
	                LogicalType::UINTEGER,
	                LogicalType::VARCHAR,
	                LogicalType::LIST(LogicalType::VARCHAR),
	                LogicalType::LIST(LogicalType::UINTEGER),
	                LogicalType::LIST(LogicalType::BOOLEAN)};

	auto bind_data = make_uniq<ProductFunctionData>();
	const auto week_start = std::to_string(IntegerValue::Get(input.inputs[0]));
	const auto week_end = std::to_string(IntegerValue::Get(input.inputs[1]));
	const auto ls_pe_id = ListValue::GetChildren(input.inputs[2]);

	for (const auto &pe_id : ls_pe_id) {
		string prefix_id = std::to_string(BigIntValue::Get(pe_id));
		reverse(prefix_id.begin(), prefix_id.end());
		bind_data->ranges.emplace_back(
		    cbt::RowRange::Closed(prefix_id + "/" + week_start + "/", prefix_id + "/" + week_end + "0"));
	}

	return std::move(bind_data);
}

DUCKDB_EXTENSION_API void ProductFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	const auto filter = cbt::Filter::PassAllFilter();
	auto &bind_data = data.bind_data->CastNoConst<ProductFunctionData>();
	auto &global_state = data.global_state->Cast<ProductGlobalState>();

	std::array<Product, 7> product_week;

	while (bind_data.ranges_idx < bind_data.ranges.size() &&
	       (bind_data.remainder.size() - bind_data.remainder_idx) < STANDARD_VECTOR_SIZE) {
		const auto &range = bind_data.ranges[bind_data.ranges_idx++];

		for (StatusOr<cbt::Row> &row_result : global_state.table->ReadRows(range, filter)) {
			if (!row_result)
				throw std::runtime_error(row_result.status().message());

			const auto &row = row_result.value();
			const auto &row_key = row.row_key();

			const auto &index_1 = row_key.find_first_of('/');
			const auto &index_2 = row_key.find_last_of('/');
			string prefix_id = row_key.substr(0, index_1);
			reverse(prefix_id.begin(), prefix_id.end());
			const auto pe_id = Value::UBIGINT(std::stoull(prefix_id));
			const auto shop_id = Value::UINTEGER(std::stoul(row_key.substr(index_2 + 1)));

			for (const auto &cell : row.cells()) {
				const date_t &date = Date::EpochToDate(cell.timestamp().count() / 1'000'000);
				const int32_t &weekday = Date::ExtractISODayOfTheWeek(date) - 1;

				auto &product_day = product_week[weekday];
				product_day.valid = true;
				product_day.pe_id = pe_id;
				product_day.shop_id = shop_id;
				product_day.date = Value::DATE(date);

				switch (cell.family_name()[0]) {
				case 'p':
					switch (cell.column_qualifier()[0]) {
					case 'p':
						product_day.price = Value(std::stod(cell.value()));
						break;
					case 'b':
						product_day.base_price = Value(std::stod(cell.value()));
						break;
					case 'u':
						product_day.unit_price = Value(std::stod(cell.value()));
						break;
					}
					break;
				case 'd':
					product_day.promo_id = Value::UINTEGER(std::stoul(cell.column_qualifier()));
					product_day.promo_text = Value(cell.value());
					break;
				case 's':
				case 'S':
					product_day.shelf.emplace_back(Value(cell.column_qualifier()));
					product_day.position.emplace_back(Value::UINTEGER(std::stoul(cell.value())));
					product_day.is_paid.emplace_back(Value::BOOLEAN(cell.family_name()[0] == 'S'));
					break;
				}
			}

			for (auto &product : product_week) {
				if (product.valid) {
					bind_data.remainder.emplace_back(product);
					product = Product();
				}
			}
		}
	}

	idx_t cardinality = 0;

	while (bind_data.remainder_idx < bind_data.remainder.size()) {
		const auto &day = bind_data.remainder[bind_data.remainder_idx++];

		output.SetValue(0, cardinality, day.pe_id);
		output.SetValue(1, cardinality, day.shop_id);
		output.SetValue(2, cardinality, day.date);
		output.SetValue(3, cardinality, day.price);
		output.SetValue(4, cardinality, day.base_price);
		output.SetValue(5, cardinality, day.unit_price);
		output.SetValue(6, cardinality, day.promo_id);
		output.SetValue(7, cardinality, day.promo_text);
		output.SetValue(8, cardinality, Value::LIST(day.shelf));
		output.SetValue(9, cardinality, Value::LIST(day.position));
		output.SetValue(10, cardinality, Value::LIST(day.is_paid));

		if (++cardinality == STANDARD_VECTOR_SIZE) {
			output.SetCardinality(cardinality);
			return;
		}
	}

	output.SetCardinality(cardinality);
}
} // namespace duckdb
