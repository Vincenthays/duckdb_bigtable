#include "common.hpp"

#include <array>
#include <atomic>
#include <optional>
#include <set>
#include <stdexcept>
#include <string_view>

using ::google::cloud::StatusOr;
using namespace duckdb::cxx;

namespace bigtable2 {

namespace {

enum ProductColumn : idx_t {
	PE_ID = 0,
	SHOP_ID = 1,
	DATE = 2,
	PRICE = 3,
	BASE_PRICE = 4,
	UNIT_PRICE = 5,
	PROMO_ID = 6,
	PROMO_TEXT = 7,
	SHELF_ID = 8,
	POSITION = 9,
	IS_PAID = 10
};

struct Product {
	uint64_t pe_id;
	uint32_t shop_id;
	int32_t date;
	std::optional<float> price;
	std::optional<float> base_price;
	std::optional<float> unit_price;
	std::optional<uint32_t> promo_id;
	std::optional<std::string> promo_text;
	std::vector<std::string> shelf;
	std::vector<uint16_t> position;
	std::vector<bool> is_paid;
};

cbt::Filter MakeFilter(const std::vector<idx_t> &column_ids) {
	std::set<std::string> families;
	for (const auto column_id : column_ids) {
		switch (column_id) {
		case PRICE:
		case BASE_PRICE:
		case UNIT_PRICE:
			families.emplace("p");
			break;
		case PROMO_ID:
		case PROMO_TEXT:
			families.emplace("d");
			break;
		case SHELF_ID:
		case POSITION:
		case IS_PAID:
			families.emplace("s|S");
			break;
		default:
			break;
		}
	}

	if (families.empty()) {
		return cbt::Filter::StripValueTransformer();
	}
	if (families.size() >= 3) {
		return cbt::Filter::PassAllFilter();
	}
	std::string regex;
	for (const auto &family : families) {
		if (!regex.empty()) {
			regex += '|';
		}
		regex += family;
	}
	return cbt::Filter::FamilyRegex(std::move(regex));
}

struct ProductGlobalState {
	const std::vector<cbt::RowSet> &units;
	const std::vector<idx_t> column_ids;
	const cbt::Filter filter;
	cbt::Table &table;

	std::atomic<idx_t> next_unit {0};

	ProductGlobalState(const ScanSpec &spec, std::vector<idx_t> column_ids_p)
	    : units(spec.units), column_ids(std::move(column_ids_p)), filter(MakeFilter(column_ids)),
	      table(SharedTable("product")) {
	}
};

struct ProductLocalState {
	idx_t remainder_idx = 0;
	std::vector<Product> remainder;
	std::array<std::optional<Product>, 7> product_week;
};

void ProductBind(TableFunction::BindInput &input) {
	auto &factory = input.GetContext().GetFactory();
	input.AddResultColumn("pe_id", factory.ParseType("UBIGINT"));
	input.AddResultColumn("shop_id", factory.ParseType("UINTEGER"));
	input.AddResultColumn("date", factory.ParseType("DATE"));
	input.AddResultColumn("price", factory.ParseType("FLOAT"));
	input.AddResultColumn("base_price", factory.ParseType("FLOAT"));
	input.AddResultColumn("unit_price", factory.ParseType("FLOAT"));
	input.AddResultColumn("promo_id", factory.ParseType("UINTEGER"));
	input.AddResultColumn("promo_text", factory.ParseType("VARCHAR"));
	input.AddResultColumn("shelf_id", factory.ParseType("VARCHAR[]"));
	input.AddResultColumn("position", factory.ParseType("USMALLINT[]"));
	input.AddResultColumn("is_paid", factory.ParseType("BOOLEAN[]"));
	input.SetOrderPreservation(OrderPreservation::NO_ORDER);
	input.SetBindData<ScanSpec>(BuildScan(input));
}

void ProductInitGlobal(TableFunction::InitGlobalInput &input) {
	const auto &spec = input.GetBindData<ScanSpec>();
	std::vector<idx_t> column_ids;
	for (idx_t i = 0; i < input.GetColumnCount(); i++) {
		column_ids.push_back(input.GetColumnIndex(i));
	}
	input.SetGlobalState<ProductGlobalState>(spec, std::move(column_ids));
	input.SetMaxThreads(std::max<idx_t>(1, spec.units.size()));
}

void ProductInitLocal(TableFunction::InitLocalInput &input) {
	input.SetLocalState<ProductLocalState>();
}

template <class T>
void WriteOptional(Vector &vec, const Product *products, idx_t count, std::optional<T> Product::*field) {
	auto *data = vec.GetDataMutable<T>();
	auto validity = vec.GetValidityMutable();
	for (idx_t i = 0; i < count; i++) {
		const auto &value = products[i].*field;
		if (value) {
			data[i] = *value;
		} else {
			validity.SetInvalid(i);
		}
	}
}

template <class T, class GETTER>
void WriteList(Vector &vec, const Product *products, idx_t count, GETTER &&get) {
	idx_t total = 0;
	for (idx_t i = 0; i < count; i++) {
		total += get(products[i]).size();
	}
	auto child = vec.GetChild(0);
	child.SetSize(total);
	auto *entries = vec.GetDataMutable<list_entry_t>();
	idx_t offset = 0;
	if constexpr (std::is_same_v<T, std::string>) {
		for (idx_t i = 0; i < count; i++) {
			const auto &items = get(products[i]);
			entries[i] = {offset, items.size()};
			for (const auto &s : items) {
				child.AssignString(offset++, s);
			}
		}
	} else {
		auto *child_data = child.GetDataMutable<T>();
		for (idx_t i = 0; i < count; i++) {
			const auto &items = get(products[i]);
			entries[i] = {offset, items.size()};
			for (idx_t j = 0; j < items.size(); j++) {
				child_data[offset++] = items[j];
			}
		}
	}
}

void ProductExec(TableFunction::ExecInput &input) {
	auto &global_state = input.GetGlobalState<ProductGlobalState>();
	auto &local_state = input.GetLocalState<ProductLocalState>();
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
			const auto pe_id = key->id;
			const auto shop_id = key->shop_id;

			for (const auto &cell : row.cells()) {
				const int64_t days = FloorDiv(cell.timestamp().count() / MICROS_PER_SECOND, SECONDS_PER_DAY);
				const int32_t weekday = WeekdayFromDays(days);

				auto &product_day = local_state.product_week[weekday];
				if (!product_day) {
					product_day.emplace();
					product_day->pe_id = pe_id;
					product_day->shop_id = shop_id;
					product_day->date = static_cast<int32_t>(days);
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
					product_day->promo_text = std::string(value);
					break;
				case 's':
				case 'S':
					if (auto pos = ParseUint16(value)) {
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

	const idx_t count = std::min(capacity, local_state.remainder.size() - local_state.remainder_idx);
	if (count == 0) {
		local_state.remainder_idx = 0;
		local_state.remainder.clear();
		return;
	}

	const auto *products = &local_state.remainder[local_state.remainder_idx];

	for (idx_t col_idx = 0; col_idx < input.GetColumnCount(); col_idx++) {
		auto vec = chunk.GetVector(col_idx);
		vec.SetSize(count);

		switch (input.GetColumnIndex(col_idx)) {
		case PE_ID: {
			auto *data = vec.GetDataMutable<uint64_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = products[i].pe_id;
			}
			break;
		}
		case SHOP_ID: {
			auto *data = vec.GetDataMutable<uint32_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = products[i].shop_id;
			}
			break;
		}
		case DATE: {
			auto *data = vec.GetDataMutable<date_t>();
			for (idx_t i = 0; i < count; i++) {
				data[i] = date_t {products[i].date};
			}
			break;
		}
		case PRICE:
			WriteOptional(vec, products, count, &Product::price);
			break;
		case BASE_PRICE:
			WriteOptional(vec, products, count, &Product::base_price);
			break;
		case UNIT_PRICE:
			WriteOptional(vec, products, count, &Product::unit_price);
			break;
		case PROMO_ID:
			WriteOptional(vec, products, count, &Product::promo_id);
			break;
		case PROMO_TEXT: {
			auto validity = vec.GetValidityMutable();
			for (idx_t i = 0; i < count; i++) {
				if (products[i].promo_text) {
					vec.AssignString(i, *products[i].promo_text);
				} else {
					validity.SetInvalid(i);
				}
			}
			break;
		}
		case SHELF_ID:
			WriteList<std::string>(vec, products, count, [](const Product &p) -> auto & { return p.shelf; });
			break;
		case POSITION:
			WriteList<uint16_t>(vec, products, count, [](const Product &p) -> auto & { return p.position; });
			break;
		case IS_PAID:
			WriteList<bool>(vec, products, count, [](const Product &p) -> auto & { return p.is_paid; });
			break;
		default:
			break;
		}
	}

	local_state.remainder_idx += count;
}

void ProductProgress(TableFunction::ProgressInput &input) {
	auto &state = input.GetGlobalState<ProductGlobalState>();
	const auto total = state.units.size();
	input.SetProgress(total == 0 ? 100.0 : 100.0 * static_cast<double>(std::min<idx_t>(state.next_unit, total)) / static_cast<double>(total));
}

} // namespace

void RegisterProduct(Extension &extension, Factory &factory) {
	auto function = TableFunction::Create(factory);
	function.SetName("product");
	function.GetSignature()
	    .AddParameter("week_start", factory.ParseType("INTEGER"))
	    .AddParameter("week_end", factory.ParseType("INTEGER"))
	    .AddParameter("pe_ids", factory.ParseType("BIGINT[]"))
	    .AddParameter("shop_ids", factory.ParseType("BIGINT[]"),
	                  Value::CreateNull(factory, factory.ParseType("BIGINT[]")));
	function.SetBindCallback(ProductBind)
	    .SetInitGlobalCallback(ProductInitGlobal)
	    .SetInitLocalCallback(ProductInitLocal)
	    .SetExecCallback(ProductExec)
	    .SetProgressCallback(ProductProgress)
	    .SetProjectionPushdown(true);
	extension.Register(function);
}

} // namespace bigtable2
