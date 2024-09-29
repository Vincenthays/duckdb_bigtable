cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME bigtable2)

# Define the project and specify the language (C++)
project(${TARGET_NAME} LANGUAGES CXX)

# Find the Google Cloud Bigtable package
find_package(google_cloud_cpp_bigtable REQUIRED)

# Include directories for headers
include_directories(src/include)

# Set the source files for the extension
set(EXTENSION_SOURCES src/bigtable2_extension.cpp)

# Define the static library target
add_library(${TARGET_NAME} STATIC ${EXTENSION_SOURCES})
# Define the loadable extension target
add_library(${TARGET_NAME}_loadable_extension SHARED ${EXTENSION_SOURCES})

# Link the Google Cloud Bigtable library to both targets
target_link_libraries(${TARGET_NAME} PUBLIC google-cloud-cpp::bigtable)
target_link_libraries(${TARGET_NAME}_loadable_extension PUBLIC google-cloud-cpp::bigtable)

# Install rules (optional, adjust as needed)
install(
  TARGETS ${TARGET_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
)
