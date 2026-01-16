#include "product.hpp"

#include "duckdb.hpp"
#include "utils.hpp"

#include <google/cloud/bigtable/table.h>
#include <optional>
#include <string_view>

using ::google::cloud::GrpcNumChannelsOption;
using ::google::cloud::Options;
using ::google::cloud::StatusOr;
namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

enum ProductColumn : column_t {
	PE_ID = 0,
	SHOP_ID = 1,
	DATE = 2,
	PRICE = 3,
	BASE_PRICE = 4,
	UNIT_PRICE = 5,
	PROMO_ID = 6,
	PROMO_TEXT = 7,
	SHELF = 8,
	POSITION = 9,
	IS_PAID = 10
};

struct Product {
	uint64_t pe_id;
	uint32_t shop_id;
	date_t date;
	std::optional<float> price;
	std::optional<float> base_price;
	std::optional<float> unit_price;
	std::optional<uint32_t> promo_id;
	std::optional<string> promo_text;
	vector<string> shelf;
	vector<uint32_t> position;
	vector<bool> is_paid;
};

struct ProductFunctionData : TableFunctionData {
	vector<uint64_t> pe_ids;
	vector<cbt::RowRange> ranges;
};

static cbt::Filter make_filter(const vector<column_t> &column_ids);

unique_ptr<FunctionData> ProductFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                             vector<LogicalType> &return_types, vector<string> &names) {
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
	const auto &ls_pe_id = ListValue::GetChildren(input.inputs[2]);

	bind_data->pe_ids.reserve(ls_pe_id.size());
	bind_data->ranges.reserve(ls_pe_id.size());

	for (const auto &p : ls_pe_id) {
		const auto pe_id = BigIntValue::Get(p);
		string prefix_id = std::to_string(pe_id);
		std::reverse(prefix_id.begin(), prefix_id.end());
		bind_data->pe_ids.emplace_back(pe_id);
		bind_data->ranges.emplace_back(
		    cbt::RowRange::Closed(prefix_id + "/" + week_start + "/", prefix_id + "/" + week_end + "0"));
	}

	return bind_data;
}

struct ProductGlobalState : GlobalTableFunctionState {
	const cbt::Filter filter;
	cbt::Table table;

	mutex lock;
	idx_t ranges_idx = 0;
	const vector<uint64_t> pe_ids;
	const vector<cbt::RowRange> ranges;
	const vector<column_t> column_ids;

	ProductGlobalState(vector<uint64_t> pe_ids_p, vector<cbt::RowRange> ranges_p, vector<column_t> column_ids_p)
	    : filter(make_filter(column_ids_p)),
	      table(cbt::MakeDataConnection(Options {}.set<GrpcNumChannelsOption>(32)),
	            cbt::TableResource("dataimpact-processing", "processing", "product")),
	      pe_ids(std::move(pe_ids_p)), ranges(std::move(ranges_p)), column_ids(std::move(column_ids_p)) {};
};

unique_ptr<GlobalTableFunctionState> ProductInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ProductFunctionData>();
	return make_uniq<ProductGlobalState>(std::move(bind_data.pe_ids), std::move(bind_data.ranges),
	                                     std::move(input.column_ids));
}

struct ProductLocalState : LocalTableFunctionState {
	idx_t remainder_idx = 0;
	vector<Product> remainder;
	std::array<std::optional<Product>, 7> product_week;
};

unique_ptr<LocalTableFunctionState> ProductInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) {
	return make_uniq<ProductLocalState>();
}

void ProductFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<ProductGlobalState>();
	auto &local_state = data.local_state->Cast<ProductLocalState>();

	while ((local_state.remainder.size() - local_state.remainder_idx) < STANDARD_VECTOR_SIZE) {
		idx_t range_idx;
		{
			lock_guard<mutex> guard(global_state.lock);
			if (global_state.ranges_idx == global_state.ranges.size()) {
				break;
			}
			range_idx = global_state.ranges_idx++;
		}

		const auto pe_id = global_state.pe_ids[range_idx];
		const auto &range = global_state.ranges[range_idx];

		for (const StatusOr<cbt::Row> &row_result : global_state.table.ReadRows(range, global_state.filter)) {
			if (!row_result)
				throw std::runtime_error(row_result.status().message());

			const auto &row = row_result.value();
			std::string_view row_key = row.row_key();
			const auto index = row_key.find_last_of('/');
			const auto shop_id_opt = ParseUint32(row_key.substr(index + 1));
			if (!shop_id_opt) {
				continue;
			}
			const auto shop_id = *shop_id_opt;

			for (const auto &cell : row.cells()) {
				const date_t date = Date::EpochToDate(cell.timestamp().count() / 1'000'000);
				const int32_t weekday = Date::ExtractISODayOfTheWeek(date) - 1;

				auto &product_day = local_state.product_week[weekday];
				if (!product_day) {
					product_day.emplace();
					product_day->pe_id = pe_id;
					product_day->shop_id = shop_id;
					product_day->date = date;
				}

				const std::string_view family = cell.family_name();
				const std::string_view qualifier = cell.column_qualifier();
				const std::string_view value = cell.value();

				switch (family[0]) {
				case 'p':
					switch (qualifier[0]) {
					case 'p':
						product_day->price = ParseFloat(value);
						break;
					case 'b':
						product_day->base_price = ParseFloat(value);
						break;
					case 'u':
						product_day->unit_price = ParseFloat(value);
						break;
					}
					break;
				case 'd':
					product_day->promo_id = ParseUint32(qualifier);
					product_day->promo_text = string(value);
					break;
				case 's':
				case 'S':
					if (auto pos = ParseUint32(value)) {
						product_day->shelf.emplace_back(qualifier);
						product_day->position.emplace_back(*pos);
						product_day->is_paid.emplace_back(family[0] == 'S');
					}
					break;
				}
			}

			for (auto &product_opt : local_state.product_week) {
				if (product_opt) {
					local_state.remainder.emplace_back(std::move(*product_opt));
					product_opt.reset();
				}
			}
		}
	}

	const idx_t count = std::min((idx_t)STANDARD_VECTOR_SIZE, local_state.remainder.size() - local_state.remainder_idx);

	if (count == 0) {
		output.SetCardinality(0);
		local_state.remainder_idx = 0;
		local_state.remainder.clear();
		return;
	}

	const auto *products = &local_state.remainder[local_state.remainder_idx];

	for (idx_t col_idx = 0; col_idx < global_state.column_ids.size(); col_idx++) {
		auto &out_vec = output.data[col_idx];
		const auto column_id = global_state.column_ids[col_idx];

		switch (static_cast<ProductColumn>(column_id)) {
		case ProductColumn::PE_ID: {
			auto data_ptr = FlatVector::GetData<uint64_t>(out_vec);
			for (idx_t i = 0; i < count; i++) {
				data_ptr[i] = products[i].pe_id;
			}
			break;
		}
		case ProductColumn::SHOP_ID: {
			auto data_ptr = FlatVector::GetData<uint32_t>(out_vec);
			for (idx_t i = 0; i < count; i++) {
				data_ptr[i] = products[i].shop_id;
			}
			break;
		}
		case ProductColumn::DATE: {
			auto data_ptr = FlatVector::GetData<date_t>(out_vec);
			for (idx_t i = 0; i < count; i++) {
				data_ptr[i] = products[i].date;
			}
			break;
		}
		case ProductColumn::PRICE: {
			auto data_ptr = FlatVector::GetData<float>(out_vec);
			auto &validity = FlatVector::Validity(out_vec);
			for (idx_t i = 0; i < count; i++) {
				if (products[i].price) {
					data_ptr[i] = *products[i].price;
				} else {
					validity.SetInvalid(i);
				}
			}
			break;
		}
		case ProductColumn::BASE_PRICE: {
			auto data_ptr = FlatVector::GetData<float>(out_vec);
			auto &validity = FlatVector::Validity(out_vec);
			for (idx_t i = 0; i < count; i++) {
				if (products[i].base_price) {
					data_ptr[i] = *products[i].base_price;
				} else {
					validity.SetInvalid(i);
				}
			}
			break;
		}
		case ProductColumn::UNIT_PRICE: {
			auto data_ptr = FlatVector::GetData<float>(out_vec);
			auto &validity = FlatVector::Validity(out_vec);
			for (idx_t i = 0; i < count; i++) {
				if (products[i].unit_price) {
					data_ptr[i] = *products[i].unit_price;
				} else {
					validity.SetInvalid(i);
				}
			}
			break;
		}
		case ProductColumn::PROMO_ID: {
			auto data_ptr = FlatVector::GetData<uint32_t>(out_vec);
			auto &validity = FlatVector::Validity(out_vec);
			for (idx_t i = 0; i < count; i++) {
				if (products[i].promo_id) {
					data_ptr[i] = *products[i].promo_id;
				} else {
					validity.SetInvalid(i);
				}
			}
			break;
		}
		case ProductColumn::PROMO_TEXT:
			for (idx_t i = 0; i < count; i++) {
				out_vec.SetValue(i, products[i].promo_text ? Value(*products[i].promo_text) : Value());
			}
			break;
		case ProductColumn::SHELF: {
			for (idx_t i = 0; i < count; i++) {
				vector<Value> vals;
				vals.reserve(products[i].shelf.size());
				for (const auto &s : products[i].shelf) {
					vals.emplace_back(s);
				}
				out_vec.SetValue(i, Value::LIST(LogicalType::VARCHAR, std::move(vals)));
			}
			break;
		}
		case ProductColumn::POSITION: {
			for (idx_t i = 0; i < count; i++) {
				vector<Value> vals;
				vals.reserve(products[i].position.size());
				for (const auto &p : products[i].position) {
					vals.emplace_back(Value::UINTEGER(p));
				}
				out_vec.SetValue(i, Value::LIST(LogicalType::UINTEGER, std::move(vals)));
			}
			break;
		}
		case ProductColumn::IS_PAID: {
			for (idx_t i = 0; i < count; i++) {
				vector<Value> vals;
				vals.reserve(products[i].is_paid.size());
				for (const auto &ip : products[i].is_paid) {
					vals.emplace_back(Value::BOOLEAN(ip));
				}
				out_vec.SetValue(i, Value::LIST(LogicalType::BOOLEAN, std::move(vals)));
			}
			break;
		}
		}
	}

	local_state.remainder_idx += count;
	output.SetCardinality(count);
}

double ProductScanProgress(ClientContext &context, const FunctionData *bind_data,
                           const GlobalTableFunctionState *global_state) {
	const auto &gstate = global_state->Cast<ProductGlobalState>();
	const auto total_count = gstate.ranges.size();
	if (total_count == 0) {
		return 100.0;
	}
	const auto completed = static_cast<double>(gstate.ranges_idx);
	return (100.0 * completed) / static_cast<double>(total_count);
}

inline static cbt::Filter make_filter(const vector<column_t> &column_ids) {
	set<string> families;

	for (const auto column_id : column_ids) {
		switch (static_cast<ProductColumn>(column_id)) {
		case ProductColumn::PRICE:
		case ProductColumn::BASE_PRICE:
		case ProductColumn::UNIT_PRICE:
			families.emplace("p");
			break;
		case ProductColumn::PROMO_ID:
		case ProductColumn::PROMO_TEXT:
			families.emplace("d");
			break;
		case ProductColumn::SHELF:
		case ProductColumn::POSITION:
		case ProductColumn::IS_PAID:
			families.emplace("s|S");
			break;
		default:
			break;
		}
	}

	if (families.empty())
		return cbt::Filter::StripValueTransformer();
	if (families.size() >= 3)
		return cbt::Filter::PassAllFilter();

	string regex;
	for (const auto &family : families) {
		if (!regex.empty())
			regex += '|';
		regex += family;
	}
	return cbt::Filter::FamilyRegex(std::move(regex));
}
} // namespace duckdb
