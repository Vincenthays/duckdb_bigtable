VCPKG_ROOT ?= $(HOME)/vcpkg
CMAKE_ARGS ?=

.PHONY: build install clean

build:
	cmake -S . -B build -G Ninja -DCMAKE_BUILD_TYPE=Release \
		-DCMAKE_TOOLCHAIN_FILE=$(VCPKG_ROOT)/scripts/buildsystems/vcpkg.cmake $(CMAKE_ARGS)
	cmake --build build

install: build
	duckdb -unsigned -c "FORCE INSTALL 'build/bigtable2.duckdb_extension'"

clean:
	rm -rf build
