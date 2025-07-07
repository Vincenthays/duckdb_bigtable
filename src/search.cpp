#include <optional>
#include <string_view>
#include <unordered_map>
#include <google/cloud/bigtable/table.h>

#include "duckdb.hpp"
#include "search.hpp"
#include "utils.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension_util.hpp"

using ::google::cloud::GrpcNumChannelsOption;
using ::google::cloud::Options;
using ::google::cloud::StatusOr;
namespace cbt = ::google::cloud::bigtable;

namespace duckdb {

enum SearchColumn : column_t {
	KEYWORD_ID = 0,
	SHOP_ID = 1,
	DATE = 2,
	POSITION = 3,
	PE_ID = 4,
	RETAILER_P_ID = 5,
	IS_PAID = 6
};
constexpr uint8_t MAX_POSITION = 200;

struct Keyword {
	uint32_t keyword_id;
	uint32_t shop_id;
	timestamp_t date;
	uint8_t position;
	std::optional<uint64_t> pe_id = std::nullopt;
	std::optional<string> retailer_p_id = std::nullopt;
	bool is_paid = false;
};

struct SearchFunctionData : TableFunctionData {
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
	const auto &ls_keyword_id = ListValue::GetChildren(input.inputs[2]);

	bind_data->keyword_ids.reserve(ls_keyword_id.size());
	bind_data->ranges.reserve(ls_keyword_id.size());

	for (const auto &k : ls_keyword_id) {
		const auto keyword_id = IntegerValue::Get(k);
		string prefix_id = std::to_string(keyword_id);
		std::reverse(prefix_id.begin(), prefix_id.end());
		bind_data->keyword_ids.push_back(keyword_id);
		bind_data->ranges.emplace_back(
		    cbt::RowRange::Closed(prefix_id + "/" + week_start + "/", prefix_id + "/" + week_end + "0"));
	}
	return bind_data;
}

struct SearchGlobalState : GlobalTableFunctionState {
	const cbt::Filter filter;
	cbt::Table table;
	mutex lock;
	idx_t ranges_idx = 0;
	const vector<uint32_t> keyword_ids;
	const vector<cbt::RowRange> ranges;
	const vector<column_t> column_ids;
	SearchGlobalState(vector<uint32_t> keyword_ids_p, vector<cbt::RowRange> ranges_p, vector<column_t> column_ids_p)
	    : filter(make_filter(column_ids_p)), table(cbt::MakeDataConnection(Options {}.set<GrpcNumChannelsOption>(32)),
	                                               cbt::TableResource("dataimpact-processing", "processing", "search")),
	      keyword_ids(std::move(keyword_ids_p)), ranges(std::move(ranges_p)), column_ids(std::move(column_ids_p)) {};
};

unique_ptr<GlobalTableFunctionState> SearchInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<SearchFunctionData>();
	return make_uniq<SearchGlobalState>(std::move(bind_data.keyword_ids), std::move(bind_data.ranges),
	                                    std::move(input.column_ids));
}

struct SearchLocalState : LocalTableFunctionState {
	idx_t remainder_idx = 0;
	vector<Keyword> remainder;
	std::unordered_map<uint32_t, Keyword> keyword_map;
};

unique_ptr<LocalTableFunctionState> SearchInitLocal(ExecutionContext &context, TableFunctionInitInput &input,
                                                    GlobalTableFunctionState *global_state) {
	return make_uniq<SearchLocalState>();
}

void SearchFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
	auto &global_state = data.global_state->Cast<SearchGlobalState>();
	auto &local_state = data.local_state->Cast<SearchLocalState>();

	while ((local_state.remainder.size() - local_state.remainder_idx) < STANDARD_VECTOR_SIZE) {
		idx_t range_idx;
		{
			lock_guard<mutex> guard(global_state.lock);
			if (global_state.ranges_idx == global_state.ranges.size()) {
				break;
			}
			range_idx = global_state.ranges_idx++;
		}

		const auto keyword_id = global_state.keyword_ids[range_idx];
		const auto &range = global_state.ranges[range_idx];

		for (const StatusOr<cbt::Row> &row_result : global_state.table.ReadRows(range, global_state.filter)) {
			if (!row_result) {
				throw std::runtime_error(row_result.status().message());
			}
			const auto &row = row_result.value();
			std::string_view row_key = row.row_key();
			const auto index = row_key.find_last_of('/');
			auto shop_id_opt = ParseUint32(row_key.substr(index + 1));
			if (!shop_id_opt) {
				continue;
			}
			const auto shop_id = *shop_id_opt;

			for (const auto &cell : row.cells()) {
				auto position_opt = ParseUint8(cell.column_qualifier());
				if (!position_opt || *position_opt == 0 || *position_opt > MAX_POSITION) {
					continue;
				}
				const auto position = *position_opt;

				std::string_view value = cell.value();
				if (value.starts_with("id_ret_pos_")) {
					continue;
				}

				const timestamp_t timestamp = Timestamp::FromEpochMicroSeconds(cell.timestamp().count());
				const date_t date = Timestamp::GetDate(timestamp);
				const int32_t weekday = Date::ExtractISODayOfTheWeek(date) - 1;
				const int32_t hour = Timestamp::GetTime(timestamp).micros / 3'600'000'000;
				const int32_t week_hour = weekday * 24 + hour;
				const uint32_t map_key = week_hour * MAX_POSITION + position - 1;

				auto &keyword =
				    local_state.keyword_map.try_emplace(map_key, Keyword {keyword_id, shop_id, timestamp, position})
				        .first->second;

				switch (cell.family_name()[0]) {
				case 'p':
					if (value.starts_with("id_ret_")) {
						keyword.retailer_p_id = string(value.substr(7));
					} else {
						keyword.pe_id = ParseUint64(value);
					}
					break;
				case 's':
					keyword.is_paid = true;
					break;
				}
			}

			local_state.remainder.reserve(local_state.remainder.size() + local_state.keyword_map.size());
			for (auto &pair : local_state.keyword_map) {
				local_state.remainder.emplace_back(std::move(pair.second));
			}
			local_state.keyword_map.clear();
		}
	}

	const idx_t count =
	    std::min((idx_t)STANDARD_VECTOR_SIZE, (idx_t)local_state.remainder.size() - local_state.remainder_idx);

	if (count == 0) {
		output.SetCardinality(0);
		local_state.remainder_idx = 0;
		local_state.remainder.clear();
		return;
	}

	for (idx_t i = 0; i < count; i++) {
		const auto &keyword = local_state.remainder[local_state.remainder_idx + i];
		for (idx_t col_idx = 0; col_idx < global_state.column_ids.size(); ++col_idx) {
			auto &out_vec = output.data[col_idx];
			const auto column_id = global_state.column_ids[col_idx];

			switch (static_cast<SearchColumn>(column_id)) {
			case SearchColumn::KEYWORD_ID:
				out_vec.SetValue(i, Value::UINTEGER(keyword.keyword_id));
				break;
			case SearchColumn::SHOP_ID:
				out_vec.SetValue(i, Value::UINTEGER(keyword.shop_id));
				break;
			case SearchColumn::DATE:
				out_vec.SetValue(i, Value::TIMESTAMP(keyword.date));
				break;
			case SearchColumn::POSITION:
				out_vec.SetValue(i, Value::UTINYINT(keyword.position));
				break;
			case SearchColumn::PE_ID:
				out_vec.SetValue(i, keyword.pe_id ? Value::UBIGINT(*keyword.pe_id) : Value());
				break;
			case SearchColumn::RETAILER_P_ID:
				out_vec.SetValue(i, keyword.retailer_p_id ? Value(*keyword.retailer_p_id) : Value());
				break;
			case SearchColumn::IS_PAID:
				out_vec.SetValue(i, Value::BOOLEAN(keyword.is_paid));
				break;
			}
		}
	}

	local_state.remainder_idx += count;
	output.SetCardinality(count);
}

double SearchScanProgress(ClientContext &context, const FunctionData *bind_data,
                          const GlobalTableFunctionState *global_state) {
	const auto &gstate = global_state->Cast<SearchGlobalState>();
	const auto total_count = gstate.ranges.size();
	if (total_count == 0) {
		return 100.0;
	}
	const auto completed = static_cast<double>(gstate.ranges_idx);
	return (100.0 * completed) / static_cast<double>(total_count);
}

inline static cbt::Filter make_filter(const vector<column_t> &column_ids) {
	set<string> families;

	for (const auto &column_id : column_ids) {
		switch (static_cast<SearchColumn>(column_id)) {
		case SearchColumn::POSITION:
		case SearchColumn::PE_ID:
		case SearchColumn::RETAILER_P_ID:
			families.emplace("p");
			break;
		case SearchColumn::IS_PAID:
			families.emplace("s");
			break;
		default:
			break;
		}
	}

	if (families.empty())
		return cbt::Filter::StripValueTransformer();
	if (families.size() >= 2)
		return cbt::Filter::PassAllFilter();

	string regex;
	for (const auto &family : families) {
		if (!regex.empty()) {
			regex += "|";
		}
		regex += family;
	}
	return cbt::Filter::FamilyRegex(std::move(regex));
}
} // namespace duckdb
