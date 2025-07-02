#include "utils.hpp"

#include <string>
#include <cstdint>
#include <optional>
#include <charconv>
#include <stdexcept>

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
	try {
		// std::stof requires a null-terminated string, so we create a temporary std::string.
		return std::stof(std::string(s));
	} catch (const std::invalid_argument &) {
		return std::nullopt;
	} catch (const std::out_of_range &) {
		return std::nullopt;
	}
}
