#include "utils.hpp"
#include "fast_float/fast_float.h"

#include <cstdint>
#include <optional>
#include <charconv>
#include <string_view>


std::optional<uint8_t> ParseUint8(std::string_view s) {
	uint8_t result;
	auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), result);
	if (ec == std::errc()) {
		return result;
	}
	return std::nullopt;
}

std::optional<uint32_t> ParseUint32(std::string_view s) {
	uint32_t result;
	auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), result);
	if (ec == std::errc()) {
		return result;
	}
	return std::nullopt;
}

std::optional<uint64_t> ParseUint64(std::string_view s) {
	uint64_t result;
	auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), result);
	if (ec == std::errc()) {
		return result;
	}
	return std::nullopt;
}

std::optional<float> ParseFloat(std::string_view s) {
	float result;
	auto [ptr, ec] = duckdb_fast_float::from_chars(s.data(), s.data() + s.size(), result);
	if (ec == std::errc()) {
		return result;
	}
	return std::nullopt;
}
