#include "common.hpp"

#include <atomic>
#include <optional>
#include <set>
#include <stdexcept>
#include <string_view>
#include <unordered_map>

using ::google::cloud::StatusOr;
using namespace duckdb::cxx;

namespace bigtable2 {

namespace {

enum SearchColumn : idx_t {
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
	int64_t timestamp_micros;
	uint8_t position;
	std::optional<uint64_t> pe_id = std::nullopt;
	std::optional<std::string> retailer_p_id = std::nullopt;
	bool is_paid = false;
};

cbt::Filter MakeFilter(const std::vector<idx_t> &column_ids) {
	std::set<std::string> families;
	for (const auto column_id : column_ids) {
		switch (column_id) {
		case POSITION:
		case PE_ID:
		case RETAILER_P_ID:
			families.emplace("p");
			break;
		case IS_PAID:
			families.emplace("s");
			break;
		default:
			break;
		}
	}

	if (families.empty()) {
		return cbt::Filter::StripValueTransformer();
	}
	if (families.size() >= 2) {
		return cbt::Filter::PassAllFilter();
	}
	return cbt::Filter::FamilyRegex(*families.begin());
}

struct SearchGlobalState {
	const std::vector<cbt::RowSet> &units;
	const std::vector<idx_t> column_ids;
	const cbt::Filter filter;
	cbt::Table &table;

	std::atomic<idx_t> next_unit {0};

	SearchGlobalState(const ScanSpec &spec, std::vector<idx_t> column_ids_p)
	    : units(spec.units), column_ids(std::move(column_ids_p)), filter(MakeFilter(column_ids)),
	      table(SharedTable("search")) {
	}
};

struct SearchLocalState {
	idx_t remainder_idx = 0;
	std::vector<Keyword> remainder;
	std::unordered_map<uint32_t, Keyword> keyword_map;
};

void SearchBind(TableFunction::BindInput &input) {
	auto &factory = input.GetContext().GetFactory();
	input.AddResultColumn("keyword_id", factory.ParseType("UINTEGER"));
	input.AddResultColumn("shop_id", factory.ParseType("UINTEGER"));
	input.AddResultColumn("date", factory.ParseType("TIMESTAMP_S"));
	input.AddResultColumn("position", factory.ParseType("UTINYINT"));
	input.AddResultColumn("pe_id", factory.ParseType("UBIGINT"));
	input.AddResultColumn("retailer_p_id", factory.ParseType("VARCHAR"));
	input.AddResultColumn("is_paid", factory.ParseType("BOOLEAN"));
	input.SetOrderPreservation(OrderPreservation::NO_ORDER);
	input.SetBindData<ScanSpec>(BuildScan(input));
}

void SearchInitGlobal(TableFunction::InitGlobalInput &input) {
	const auto &spec = input.GetBindData<ScanSpec>();
	std::vector<idx_t> column_ids;
	for (idx_t i = 0; i < input.GetColumnCount(); i++) {
		column_ids.push_back(input.GetColumnIndex(i));
	}
	input.SetGlobalState<SearchGlobalState>(spec, std::move(column_ids));
	input.SetMaxThreads(std::max<idx_t>(1, spec.units.size()));
}

void SearchInitLocal(TableFunction::InitLocalInput &input) {
	input.SetLocalState<SearchLocalState>();
}

void SearchExec(TableFunction::ExecInput &input) {
	auto &global_state = input.GetGlobalState<SearchGlobalState>();
	auto &local_state = input.GetLocalState<SearchLocalState>();
	auto chunk = input.GetOutputChunk();
	const idx_t capacity = chunk.GetCapacity();

	while ((local_state.remainder.size() - local_state.remainder_idx) < capacity) {
		const idx_t unit_idx = global_state.next_unit.fetch_add(1, std::memory_order_relaxed);
		if (unit_idx >= global_state.units.size()) {
			break;
		}

		const auto &rows = global_state.units[unit_idx];

		for (const StatusOr<cbt::Row> &row_result : global_state.table.ReadRows(rows, global_state.filter)) {
			if (!row_result) {
				throw std::runtime_error(row_result.status().message());
			}
			const auto &row = row_result.value();
			const auto key = ParseRowKey(row.row_key());
			if (!key) {
				continue;
			}
			const auto keyword_id = static_cast<uint32_t>(key->id);
			const auto shop_id = key->shop_id;

			for (const auto &cell : row.cells()) {
				const auto position_opt = ParseUint8(cell.column_qualifier());
				if (!position_opt || *position_opt == 0 || *position_opt > MAX_POSITION) {
					continue;
				}
				const auto position = *position_opt;

				const std::string_view value = cell.value();
				if (value.starts_with("id_ret_pos_")) {
					continue;
				}

				const int64_t micros = cell.timestamp().count();
				const int64_t days = FloorDiv(micros, MICROS_PER_DAY);
				const int32_t weekday = WeekdayFromDays(days);
				const int32_t hour = static_cast<int32_t>((micros - days * MICROS_PER_DAY) / MICROS_PER_HOUR);
				const int32_t week_hour = weekday * 24 + hour;
				const uint32_t map_key = week_hour * MAX_POSITION + position - 1;

				auto &keyword =
				    local_state.keyword_map.try_emplace(map_key, Keyword {keyword_id, shop_id, micros, position})
				        .first->second;

				switch (cell.family_name()[0]) {
				case 'p':
					if (value.starts_with("id_ret_")) {
						keyword.retailer_p_id = std::string(value.substr(7));
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

	const idx_t count = std::min(capacity, local_state.remainder.size() - local_state.remainder_idx);
	if (count == 0) {
		local_state.remainder_idx = 0;
		local_state.remainder.clear();
		return;
	}

	const auto *keywords = &local_state.remainder[local_state.remainder_idx];

	for (idx_t col_idx = 0; col_idx < input.GetColumnCount(); col_idx++) {
		auto vec = chunk.GetVector(col_idx);
		vec.SetSize(count);

		switch (input.GetColumnIndex(col_idx)) {
		case KEYWORD_ID: {
			auto *data = vec.GetDataMutable<uint32_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = keywords[i].keyword_id;
			}
			break;
		}
		case SHOP_ID: {
			auto *data = vec.GetDataMutable<uint32_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = keywords[i].shop_id;
			}
			break;
		}
		case DATE: {
			auto *data = vec.GetDataMutable<timestamp_s_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = timestamp_s_t {keywords[i].timestamp_micros / MICROS_PER_SECOND};
			}
			break;
		}
		case POSITION: {
			auto *data = vec.GetDataMutable<uint8_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = keywords[i].position;
			}
			break;
		}
		case PE_ID: {
			auto *data = vec.GetDataMutable<uint64_t>();
			auto validity = vec.GetValidityMutable();
			for (idx_t i = 0; i < count; i++) {
				if (keywords[i].pe_id) {
					data[i] = *keywords[i].pe_id;
				} else {
					validity.SetInvalid(i);
				}
			}
			break;
		}
		case RETAILER_P_ID: {
			auto validity = vec.GetValidityMutable();
			for (idx_t i = 0; i < count; i++) {
				if (keywords[i].retailer_p_id) {
					vec.AssignString(i, *keywords[i].retailer_p_id);
				} else {
					validity.SetInvalid(i);
				}
			}
			break;
		}
		case IS_PAID: {
			auto *data = vec.GetDataMutable<bool>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = keywords[i].is_paid;
			}
			break;
		}
		default:
			break;
		}
	}

	local_state.remainder_idx += count;
}

void SearchProgress(TableFunction::ProgressInput &input) {
	auto &state = input.GetGlobalState<SearchGlobalState>();
	const auto total = state.units.size();
	input.SetProgress(total == 0 ? 100.0 : 100.0 * static_cast<double>(std::min<idx_t>(state.next_unit, total)) / static_cast<double>(total));
}

} // namespace

void RegisterSearch(Extension &extension, Factory &factory) {
	auto function = TableFunction::Create(factory);
	function.SetName("search");
	function.GetSignature()
	    .AddParameter("week_start", factory.ParseType("INTEGER"))
	    .AddParameter("week_end", factory.ParseType("INTEGER"))
	    .AddParameter("keyword_ids", factory.ParseType("INTEGER[]"))
	    .AddParameter("shop_ids", factory.ParseType("BIGINT[]"),
	                  Value::CreateNull(factory, factory.ParseType("BIGINT[]")));
	function.SetBindCallback(SearchBind)
	    .SetInitGlobalCallback(SearchInitGlobal)
	    .SetInitLocalCallback(SearchInitLocal)
	    .SetExecCallback(SearchExec)
	    .SetProgressCallback(SearchProgress)
	    .SetProjectionPushdown(true);
	extension.Register(function);
}

} // namespace bigtable2
