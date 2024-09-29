cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME bigtable2)

# Add project with CXX as language
project(${TARGET_NAME} LANGUAGES CXX)

find_package(google_cloud_cpp_bigtable REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

include_directories(src/include)

set(EXTENSION_SOURCES src/bigtable2_extension.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

#target_link_libraries(${TARGET_NAME} PRIVATE google-cloud-cpp::bigtable)
target_link_libraries(${LOADABLE_EXTENSION_NAME} google-cloud-cpp::bigtable)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
