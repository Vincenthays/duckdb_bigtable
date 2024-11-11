cmake_minimum_required(VERSION 3.10)

set(EXT_NAME bigtable2)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Define the project
project(${EXT_NAME})

# Include necessary directories
include_directories(src/include)

# Find Google Cloud Bigtable
find_package(google_cloud_cpp_bigtable REQUIRED)

# Use DuckDB macros to build the static and loadable extensions
build_static_extension(${EXT_NAME} src/bigtable2_extension.cpp})
build_loadable_extension(${EXT_NAME} " " src/bigtable2_extension.cpp)

# Use plain signature for target_link_libraries to match DuckDB's style
target_link_libraries(${EXT_NAME}_extension google-cloud-cpp::bigtable)
target_link_libraries(${EXT_NAME}_loadable_extension google-cloud-cpp::bigtable)

# Install rules (optional, adjust as needed)
install(
  TARGETS ${EXT_NAME}_extension
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
