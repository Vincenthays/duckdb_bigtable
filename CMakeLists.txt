cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME bigtable2)

# Define the project and specify the language (C++)
project(${TARGET_NAME} LANGUAGES CXX)

# Include necessary directories
include_directories(src/include)

# Find Google Cloud Bigtable
find_package(google_cloud_cpp_bigtable REQUIRED)

# Set the source files for the extension
set(EXTENSION_SOURCES src/bigtable2_extension.cpp)

# Use DuckDB macros to build the static and loadable extensions
build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Use plain signature for target_link_libraries to match DuckDB's style
target_link_libraries(${TARGET_NAME}_extension google-cloud-cpp::bigtable)
target_link_libraries(${TARGET_NAME}_loadable_extension google-cloud-cpp::bigtable)

# Install rules (optional, adjust as needed)
install(
  TARGETS ${TARGET_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
