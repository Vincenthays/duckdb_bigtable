#pragma once

#include <cstdint>
#include <optional>
#include <string_view>

// Declarations for parsing utility functions.
// These functions are defined in utils.cpp and can be used across the project.

// Parses a string_view into an optional uint8_t.
std::optional<uint8_t> ParseUint8(std::string_view s);

// Parses a string_view into an optional uint32_t.
std::optional<uint32_t> ParseUint32(std::string_view s);

// Parses a string_view into an optional uint64_t.
std::optional<uint64_t> ParseUint64(std::string_view s);

// Parses a string_view into an optional float.
std::optional<float> ParseFloat(std::string_view s);
