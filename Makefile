PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=bigtable2
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

EXT_FLAGS=-DBUILD_SHELL=1

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Build only the loadable extension (skips the DuckDB shell, unittest and other extensions)
extension: ${EXTENSION_CONFIG_STEP}
	mkdir -p build/release
	cmake $(GENERATOR) $(BUILD_FLAGS) $(EXT_RELEASE_FLAGS) $(VCPKG_MANIFEST_FLAGS) -DCMAKE_BUILD_TYPE=Release -S $(DUCKDB_SRCDIR) -B build/release
	cmake --build build/release --config Release --target bigtable2_loadable_extension
