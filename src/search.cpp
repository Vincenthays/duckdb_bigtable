#include "duckdb.hpp"
#include "search.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"
#include <google/cloud/bigtable/table.h>

using ::google::cloud::GrpcNumChannelsOption;
using ::google::cloud::Options;
using ::google::cloud::StatusOr;
namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

struct Keyword final {
	bool valid = false;
	Value keyword_id;
	Value shop_id;
	Value date;
	Value position;
	Value pe_id;
	Value retailer_p_id;
	Value is_paid = false;
};

struct SearchFunctionData final : TableFunctionData {
	vector<uint32_t> keyword_ids;
	vector<cbt::RowRange> ranges;
};

static cbt::Filter make_filter(const vector<column_t> &column_ids);

unique_ptr<FunctionData> SearchFunctionBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
	names = {"keyword_id", "shop_id", "date", "position", "pe_id", "retailer_p_id", "is_paid"};

	return_types = {LogicalType::UINTEGER, LogicalType::UINTEGER, LogicalType::TIMESTAMP_S, LogicalType::UTINYINT,
	                LogicalType::UBIGINT,  LogicalType::VARCHAR,  LogicalType::BOOLEAN};

	auto bind_data = make_uniq<SearchFunctionData>();
	const auto week_start = std::to_string(IntegerValue::Get(input.inputs[0]));
	const auto week_end = std::to_string(IntegerValue::Get(input.inputs[1]));
	const auto ls_keyword_id = ListValue::GetChildren(input.inputs[2]);

	for (const auto &k : ls_keyword_id) {
		const auto keyword_id = IntegerValue::Get(k);
		string prefix_id = std::to_string(keyword_id);
		reverse(prefix_id.begin(), prefix_id.end());
		bind_data->keyword_ids.push_back(keyword_id);
		bind_data->ranges.emplace_back(
		    cbt::RowRange::Closed(prefix_id + "/" + week_start + "/", prefix_id + "/" + week_end + "0"));
	}

	return bind_data;
}

struct SearchGlobalState final : GlobalTableFunctionState {
	const cbt::Filter filter;
	cbt::Table table = cbt::Table(cbt::MakeDataConnection(Options {}.set<GrpcNumChannelsOption>(32)),
	                              cbt::TableResource("dataimpact-processing", "processing", "search"));

	mutex lock;
	idx_t ranges_idx = 0;
	const vector<uint32_t> keyword_ids;
	const vector<cbt::RowRange> ranges;

	const vector<column_t> column_ids;

	const idx_t max_threads;
	idx_t MaxThreads() const override {
		return max_threads;
	}

	SearchGlobalState(vector<uint32_t> keyword_ids, vector<cbt::RowRange> ranges, vector<column_t> column_ids)
	    : filter(make_filter(column_ids)), keyword_ids(keyword_ids), ranges(ranges), column_ids(column_ids),
	      max_threads(ranges.size()) {};
};

unique_ptr<GlobalTableFunctionState> SearchInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<SearchFunctionData>();
	return make_uniq<SearchGlobalState>(std::move(bind_data.keyword_ids), std::move(bind_data.ranges),
	                                    std::move(input.column_ids));
}

struct SearchLocalState final : LocalTableFunctionState {
	idx_t remainder_idx = 0;
	vector<Keyword> remainder;
	std::array<Keyword, 200 * 7 * 24> keyword_week;
};

unique_ptr<LocalTableFunctionState> SearchInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                    GlobalTableFunctionState *global_state) {
	return make_uniq<SearchLocalState>();
}

void SearchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<SearchGlobalState>();
	auto &local_state = data.local_state->Cast<SearchLocalState>();

	while ((local_state.remainder.size() - local_state.remainder_idx) < STANDARD_VECTOR_SIZE) {
		// Get next range if any
		global_state.lock.lock();
		if (global_state.ranges_idx == global_state.ranges.size()) {
			global_state.lock.unlock();
			break;
		}
		const auto range_idx = global_state.ranges_idx++;
		global_state.lock.unlock();

		const auto &keyword_id = Value::UINTEGER(global_state.keyword_ids[range_idx]);
		const auto &range = global_state.ranges[range_idx];

		for (const StatusOr<cbt::Row> &row_result : global_state.table.ReadRows(range, global_state.filter)) {
			if (!row_result)
				throw std::runtime_error(row_result.status().message());

			const auto &row = row_result.value();
			const auto &row_key = row.row_key();
			const auto index = row_key.find_last_of('/');
			const auto shop_id = Value::UINTEGER(std::stoul(row_key.substr(index + 1)));

			for (const auto &cell : row.cells()) {
				const int32_t &position = std::stoul(cell.column_qualifier());

				if (position > 200 || cell.value().starts_with("id_ret_pos_"))
					continue;

				const timestamp_t timestamp = Timestamp::FromEpochMicroSeconds(cell.timestamp().count());
				const date_t date = Timestamp::GetDate(timestamp);
				const int32_t weekday = Date::ExtractISODayOfTheWeek(date) - 1;
				const int32_t hour = Timestamp::GetTime(timestamp).micros / 3'600'000'000;
				const int32_t week_hour = weekday * 24 + hour;

				auto &keyword_day = local_state.keyword_week[200 * week_hour + position - 1];
				keyword_day.valid = true;
				keyword_day.keyword_id = keyword_id;
				keyword_day.shop_id = shop_id;
				keyword_day.date = Value::TIMESTAMP(timestamp);
				keyword_day.position = Value::UTINYINT(position);

				if (global_state.filter == cbt::Filter::StripValueTransformer())
					continue;

				switch (cell.family_name()[0]) {
				case 'p':
					if (cell.value().starts_with("id_ret_"))
						keyword_day.retailer_p_id = Value(cell.value().substr(7));
					else
						keyword_day.pe_id = Value::UBIGINT(std::stoull(cell.value()));
					break;
				case 's':
					keyword_day.is_paid = Value::BOOLEAN(true);
					break;
				}
			}

			for (auto &keyword : local_state.keyword_week) {
				if (keyword.valid) {
					local_state.remainder.emplace_back(std::move(keyword));
					keyword = Keyword();
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
				output.SetValue(i, cardinality, day.keyword_id);
				break;
			case 1:
				output.SetValue(i, cardinality, day.shop_id);
				break;
			case 2:
				output.SetValue(i, cardinality, day.date);
				break;
			case 3:
				output.SetValue(i, cardinality, day.position);
				break;
			case 4:
				output.SetValue(i, cardinality, day.pe_id);
				break;
			case 5:
				output.SetValue(i, cardinality, day.retailer_p_id);
				break;
			case 6:
				output.SetValue(i, cardinality, day.is_paid);
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

double SearchScanProgress(ClientContext &context, const FunctionData *bind_data,
                          const GlobalTableFunctionState *global_state) {
	const auto &gstate = global_state->Cast<SearchGlobalState>();
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
			filters.emplace("s");
			break;
		}
	}

	switch (filters.size()) {
	case 1:
		return cbt::Filter::FamilyRegex(*filters.begin());
	case 2:
		return cbt::Filter::PassAllFilter();
	default:
		return cbt::Filter::StripValueTransformer();
	}
}
} // namespace duckdb
