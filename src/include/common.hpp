#pragma once

#include "duckdb_cpp.hpp"
#include "utils.hpp"

#include <google/cloud/bigtable/table.h>

#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace bigtable2 {

namespace cbt = ::google::cloud::bigtable;
using duckdb::cxx::idx_t;

constexpr int64_t MICROS_PER_SECOND = 1'000'000;
constexpr int64_t MICROS_PER_HOUR = 3'600'000'000;
constexpr int64_t SECONDS_PER_DAY = 86'400;
constexpr int64_t MICROS_PER_DAY = SECONDS_PER_DAY * MICROS_PER_SECOND;

// Exact row keys fetched per ReadRows call when shop ids are given.
constexpr size_t KEYS_PER_BATCH = 64;

inline int64_t FloorDiv(int64_t a, int64_t b) {
	int64_t q = a / b;
	return (a % b != 0 && ((a < 0) != (b < 0))) ? q - 1 : q;
}

// ISO day of the week minus one: 0 = Monday ... 6 = Sunday. 1970-01-01 was a Thursday.
inline int32_t WeekdayFromDays(int64_t days) {
	return static_cast<int32_t>(((days % 7) + 7 + 3) % 7);
}

// Row keys look like "<reversed id>/<week>/<shop_id>".
struct RowKeyParts {
	uint64_t id;
	uint32_t shop_id;
};

inline std::optional<RowKeyParts> ParseRowKey(std::string_view key) {
	const auto first = key.find('/');
	const auto last = key.find_last_of('/');
	if (first == std::string_view::npos || first == last || first > 20) {
		return std::nullopt;
	}
	char buf[20];
	for (size_t i = 0; i < first; i++) {
		buf[i] = key[first - 1 - i];
	}
	const auto id = ParseUint64(std::string_view(buf, first));
	const auto shop_id = ParseUint32(key.substr(last + 1));
	if (!id || !shop_id) {
		return std::nullopt;
	}
	return RowKeyParts {*id, *shop_id};
}

// Weeks are YYYYWW; skip the non-existent WW > 53 when crossing a year boundary.
inline std::vector<int32_t> WeeksBetween(int32_t start, int32_t end) {
	std::vector<int32_t> weeks;
	for (int32_t w = start; w <= end;) {
		weeks.push_back(w);
		w++;
		if (w % 100 > 53) {
			w = (w / 100 + 1) * 100 + 1;
		}
	}
	return weeks;
}

// The scan is split into independent work units, each one ReadRows call, so every thread streams in parallel.
struct ScanSpec {
	std::vector<uint64_t> ids;
	std::vector<uint32_t> shop_ids;
	std::vector<cbt::RowSet> units;
};

inline ScanSpec BuildScan(duckdb::cxx::FunctionBindInput &input) {
	ScanSpec spec;
	const auto week_start = input.GetConstantArgument(0).Get<int32_t>();
	const auto week_end = input.GetConstantArgument(1).Get<int32_t>();

	const auto ids = input.GetConstantArgument(2);
	std::vector<std::string> prefixes;
	for (idx_t i = 0; i < ids.GetChildCount(); i++) {
		const auto id = static_cast<uint64_t>(ids.GetChild(i).Get<int64_t>());
		spec.ids.push_back(id);
		std::string prefix = std::to_string(id);
		std::reverse(prefix.begin(), prefix.end());
		prefixes.push_back(std::move(prefix));
	}

	auto shops = input.TryGetConstantArgument(3);
	if (input.GetArgCount() > 3 && shops && !shops->IsNull()) {
		// Exact keys (only week_start, as before), batched into multi-key ReadRows calls.
		const auto week = std::to_string(week_start);
		cbt::RowSet batch;
		size_t in_batch = 0;
		for (idx_t s = 0; s < shops->GetChildCount(); s++) {
			const auto shop_id = static_cast<uint32_t>(shops->GetChild(s).Get<int64_t>());
			spec.shop_ids.push_back(shop_id);
			for (const auto &p : prefixes) {
				batch.Append(p + "/" + week + "/" + std::to_string(shop_id));
				if (++in_batch == KEYS_PER_BATCH) {
					spec.units.push_back(std::move(batch));
					batch = cbt::RowSet();
					in_batch = 0;
				}
			}
		}
		if (in_batch > 0) {
			spec.units.push_back(std::move(batch));
		}
	} else {
		// One unit per id, week and leading digit of the shop id.
		for (const auto week : WeeksBetween(week_start, week_end)) {
			const auto week_str = std::to_string(week);
			for (const auto &p : prefixes) {
				for (char digit = '0'; digit <= '9'; digit++) {
					spec.units.emplace_back(cbt::RowRange::Prefix(p + "/" + week_str + "/" + digit));
				}
			}
		}
	}
	return spec;
}

// One connection per process for each table, shared by every query: avoids a TLS handshake on all channels per query.
// Intentionally leaked so it is never torn down while gRPC threads may still be running at exit.
inline cbt::Table &SharedTable(const std::string &table_id) {
	static auto *connection = new std::shared_ptr<cbt::DataConnection>(
	    cbt::MakeDataConnection(google::cloud::Options {}.set<google::cloud::GrpcNumChannelsOption>(32)));
	static auto *product = new cbt::Table(*connection, cbt::TableResource("dataimpact-processing", "processing", "product"));
	static auto *search = new cbt::Table(*connection, cbt::TableResource("dataimpact-processing", "processing", "search"));
	return table_id == "product" ? *product : *search;
}

void RegisterProduct(duckdb::cxx::Extension &extension, duckdb::cxx::Factory &factory);
void RegisterSearch(duckdb::cxx::Extension &extension, duckdb::cxx::Factory &factory);

} // namespace bigtable2
