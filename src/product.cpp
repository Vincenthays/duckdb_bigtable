#include "duckdb.hpp"
#include "product.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <google/cloud/bigtable/table.h>

using ::google::cloud::GrpcNumChannelsOption;
using ::google::cloud::Options;
using ::google::cloud::StatusOr;
namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

struct Product final {
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

struct ProductFunctionData final : TableFunctionData {
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
	const auto ls_pe_id = ListValue::GetChildren(input.inputs[2]);

	for (const auto &p : ls_pe_id) {
		const auto pe_id = BigIntValue::Get(p);
		string prefix_id = std::to_string(pe_id);
		reverse(prefix_id.begin(), prefix_id.end());
		bind_data->pe_ids.emplace_back(pe_id);
		bind_data->ranges.emplace_back(
		    cbt::RowRange::Closed(prefix_id + "/" + week_start + "/", prefix_id + "/" + week_end + "0"));
	}

	return bind_data;
}

struct ProductGlobalState final : GlobalTableFunctionState {
	const cbt::Filter filter;
	cbt::Table table = cbt::Table(cbt::MakeDataConnection(Options {}.set<GrpcNumChannelsOption>(32)),
	                              cbt::TableResource("dataimpact-processing", "processing", "product"));

	mutex lock;
	idx_t ranges_idx = 0;
	const vector<uint64_t> pe_ids;
	const vector<cbt::RowRange> ranges;

	const vector<column_t> column_ids;

	const idx_t max_threads;
	idx_t MaxThreads() const override {
		return max_threads;
	}

	ProductGlobalState(vector<uint64_t> pe_ids, vector<cbt::RowRange> ranges, vector<column_t> column_ids)
	    : filter(make_filter(column_ids)), pe_ids(pe_ids), ranges(ranges), column_ids(column_ids),
	      max_threads(ranges.size()) {};
};

unique_ptr<GlobalTableFunctionState> ProductInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ProductFunctionData>();
	return make_uniq<ProductGlobalState>(std::move(bind_data.pe_ids), std::move(bind_data.ranges),
	                                     std::move(input.column_ids));
}

struct ProductLocalState final : LocalTableFunctionState {
	idx_t remainder_idx = 0;
	vector<Product> remainder;
	std::array<Product, 7> product_week;
};

unique_ptr<LocalTableFunctionState> ProductInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                     GlobalTableFunctionState *global_state) {
	return make_uniq<ProductLocalState>();
}

void ProductFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<ProductGlobalState>();
	auto &local_state = data.local_state->Cast<ProductLocalState>();

	while ((local_state.remainder.size() - local_state.remainder_idx) < STANDARD_VECTOR_SIZE) {
		// Get next range if any
		global_state.lock.lock();
		if (global_state.ranges_idx == global_state.ranges.size()) {
			global_state.lock.unlock();
			break;
		}
		const auto range_idx = global_state.ranges_idx++;
		global_state.lock.unlock();

		const auto &pe_id = Value::UBIGINT(global_state.pe_ids[range_idx]);
		const auto &range = global_state.ranges[range_idx];

		for (const StatusOr<cbt::Row> &row_result : global_state.table.ReadRows(range, global_state.filter)) {
			if (!row_result)
				throw std::runtime_error(row_result.status().message());

			const auto &row = row_result.value();
			const auto &row_key = row.row_key();
			const auto index = row_key.find_last_of('/');
			const auto shop_id = Value::UINTEGER(std::stoul(row_key.substr(index + 1)));

			for (const auto &cell : row.cells()) {
				const date_t &date = Date::EpochToDate(cell.timestamp().count() / 1'000'000);
				const int32_t &weekday = Date::ExtractISODayOfTheWeek(date) - 1;

				auto &product_day = local_state.product_week[weekday];
				product_day.valid = true;
				product_day.pe_id = pe_id;
				product_day.shop_id = shop_id;
				product_day.date = Value::DATE(date);

				if (global_state.filter == cbt::Filter::StripValueTransformer())
					continue;

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

			for (auto &product : local_state.product_week) {
				if (product.valid) {
					local_state.remainder.emplace_back(std::move(product));
					product = Product();
				}
			}
		}
	}

	idx_t cardinality = 0;
	const auto column_count = global_state.column_ids.size();

	while (local_state.remainder_idx < local_state.remainder.size()) {
		const auto &day = local_state.remainder[local_state.remainder_idx++];

		for (idx_t i = 0; i < column_count; i++) {
			switch (global_state.column_ids[i]) {
			case 0:
				output.SetValue(i, cardinality, day.pe_id);
				break;
			case 1:
				output.SetValue(i, cardinality, day.shop_id);
				break;
			case 2:
				output.SetValue(i, cardinality, day.date);
				break;
			case 3:
				output.SetValue(i, cardinality, day.price);
				break;
			case 4:
				output.SetValue(i, cardinality, day.base_price);
				break;
			case 5:
				output.SetValue(i, cardinality, day.unit_price);
				break;
			case 6:
				output.SetValue(i, cardinality, day.promo_id);
				break;
			case 7:
				output.SetValue(i, cardinality, day.promo_text);
				break;
			case 8:
				output.SetValue(i, cardinality, Value::LIST(day.shelf));
				break;
			case 9:
				output.SetValue(i, cardinality, Value::LIST(day.position));
				break;
			case 10:
				output.SetValue(i, cardinality, Value::LIST(day.is_paid));
				break;
			}
		}

		if (++cardinality == STANDARD_VECTOR_SIZE) {
			output.SetCardinality(cardinality);
			return;
		}
	}

	output.SetCardinality(cardinality);
}

double ProductScanProgress(ClientContext &context, const FunctionData *bind_data,
                           const GlobalTableFunctionState *global_state) {
	const auto &gstate = global_state->Cast<ProductGlobalState>();
	const auto total_count = gstate.ranges.size();
	if (total_count == 0)
		return 100.0;

	return (100.0 * (static_cast<double>(gstate.ranges_idx) + 1.0)) / static_cast<double>(total_count);
}

static cbt::Filter make_filter(const vector<column_t> &column_ids) {
	set<string> filters;

	for (const auto &column_id : column_ids) {
		switch (column_id) {
		case 3:
		case 4:
		case 5:
			filters.emplace("p");
			break;
		case 6:
		case 7:
			filters.emplace("d");
			break;
		case 8:
		case 9:
		case 10:
			filters.emplace("s|S");
			break;
		}
	}

	switch (filters.size()) {
	case 1:
		return cbt::Filter::FamilyRegex(*filters.begin());
	case 2:
		return cbt::Filter::FamilyRegex(*filters.begin() + "|" + *filters.end());
	case 3:
		return cbt::Filter::PassAllFilter();
	default:
		return cbt::Filter::StripValueTransformer();
	}
}
} // namespace duckdb
