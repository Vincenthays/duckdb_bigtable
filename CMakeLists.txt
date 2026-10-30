cmake_minimum_required(VERSION 3.22)

project(bigtable2 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 26)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_VISIBILITY_PRESET hidden)
set(CMAKE_VISIBILITY_INLINES_HIDDEN ON)

# The DuckDB build this extension is pinned to (C API v2 is unstable, so it only loads on this exact version).
set(DUCKDB_VERSION "v2.0.0" CACHE STRING "DuckDB version the extension targets")
set(EXTENSION_VERSION "1.0.0" CACHE STRING "Extension version")
if(NOT DUCKDB_PLATFORM)
  if(APPLE)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
      set(DUCKDB_PLATFORM "osx_arm64")
    else()
      set(DUCKDB_PLATFORM "osx_amd64")
    endif()
  elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64|aarch64")
    set(DUCKDB_PLATFORM "linux_arm64")
  else()
    set(DUCKDB_PLATFORM "linux_amd64")
  endif()
endif()

find_package(google_cloud_cpp_bigtable REQUIRED)

add_library(
  bigtable2 MODULE
  src/bigtable2_extension.cpp src/product.cpp src/search.cpp src/utils.cpp
  third_party/duckdb/duckdb_cpp.cpp)
target_include_directories(bigtable2 PRIVATE src/include third_party/duckdb
                                             third_party/fast_float)
target_compile_definitions(
  bigtable2
  PRIVATE DUCKDB_EXTENSION_NAME=bigtable2 DUCKDB_CPP_API_LOADABLE
          DUCKDB_EXTENSION_API_VERSION_UNSTABLE=${DUCKDB_VERSION})
target_link_libraries(bigtable2 PRIVATE google-cloud-cpp::bigtable)
set_target_properties(
  bigtable2
  PROPERTIES PREFIX ""
             SUFFIX ".duckdb_extension.raw"
             LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

# Append the DuckDB extension footer (platform, version, ABI type).
set(EXTENSION_FILE ${CMAKE_BINARY_DIR}/bigtable2.duckdb_extension)
file(WRITE ${CMAKE_BINARY_DIR}/duckdb_platform_out "${DUCKDB_PLATFORM}")
execute_process(COMMAND printf "\\0" OUTPUT_FILE ${CMAKE_BINARY_DIR}/null.txt)
add_custom_command(
  TARGET bigtable2
  POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:bigtable2> ${EXTENSION_FILE}
  COMMAND
    ${CMAKE_COMMAND} -DEXTENSION=${EXTENSION_FILE}
    -DPLATFORM_FILE=${CMAKE_BINARY_DIR}/duckdb_platform_out
    -DVERSION_FIELD=${DUCKDB_VERSION} -DEXTENSION_VERSION=${EXTENSION_VERSION}
    -DABI_TYPE=C_STRUCT_UNSTABLE -DNULL_FILE=${CMAKE_BINARY_DIR}/null.txt -P
    ${CMAKE_SOURCE_DIR}/cmake/append_metadata.cmake
  VERBATIM)
