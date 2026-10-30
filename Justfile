bucket := "gs://di_duckdb_extension"
vcpkg_root := env("VCPKG_ROOT", env("HOME") + "/vcpkg")

# Vendor the C API v2 headers and C++ wrapper of a DuckDB release (the extension only loads on that exact version)
duckdb-headers DUCKDB_VERSION:
    #!/usr/bin/env sh
    set -e
    base="https://raw.githubusercontent.com/duckdb/duckdb/v{{DUCKDB_VERSION}}"
    for f in src/include/duckdb_v2.h src/include/duckdb_extension_v2.h tools/cpp/duckdb_cpp.hpp tools/cpp/duckdb_cpp.cpp tools/cpp/duckdb_cpp_extension.hpp; do
        curl -fsSL "$base/$f" -o "third_party/duckdb/$(basename $f)"
    done
    echo "headers synced to DuckDB v{{DUCKDB_VERSION}}"

release DUCKDB_VERSION="2.0.0":
    make build VCPKG_ROOT={{vcpkg_root}} CMAKE_ARGS="-DDUCKDB_VERSION=v{{DUCKDB_VERSION}}"

install DUCKDB_VERSION="2.0.0": (release DUCKDB_VERSION)
    duckdb -unsigned -c "FORCE INSTALL 'build/bigtable2.duckdb_extension'"

[macos]
deploy DUCKDB_VERSION: (duckdb-headers DUCKDB_VERSION) (release DUCKDB_VERSION)
    gzip -c build/bigtable2.duckdb_extension | gsutil cp - {{bucket}}/v{{DUCKDB_VERSION}}/osx_arm64/bigtable2.duckdb_extension.gz

[linux]
deploy DUCKDB_VERSION: (duckdb-headers DUCKDB_VERSION)
    #!/usr/bin/env sh
    set -e
    for platform in linux_amd64 linux_amd64_musl; do
        docker build --platform linux/amd64 -f Dockerfile_$platform -t duckdb_extension_$platform .
        docker run -i -v /home/dataimpact/gs.json:/app/gs.json duckdb_extension_$platform bash <<EOS
            gcloud auth activate-service-account --key-file /app/gs.json
            gsutil cp bigtable2.duckdb_extension.gz {{bucket}}/v{{DUCKDB_VERSION}}/$platform/bigtable2.duckdb_extension.gz
    EOS
    done

# Build the Linux extensions locally (works on macOS too, through emulation) into dist/
build-linux:
    #!/usr/bin/env sh
    set -e
    mkdir -p dist
    for platform in linux_amd64 linux_amd64_musl; do
        docker build --platform linux/amd64 -f Dockerfile_$platform -t duckdb_extension_$platform .
        docker run --rm --platform linux/amd64 -v "$PWD/dist:/out" duckdb_extension_$platform \
            cp bigtable2.duckdb_extension.gz /out/bigtable2.duckdb_extension-$platform.gz
    done

duckdb := "duckdb -unsigned --init /dev/null -c \"FORCE INSTALL 'build/bigtable2.duckdb_extension'; LOAD bigtable2\" -c"

test: release
    {{duckdb}} "FROM product(2024_20, 2024_20, [1124000100000]) LIMIT 5"
    {{duckdb}} "FROM product(2024_20, 2024_20, [1124000100000], [41188])"
    {{duckdb}} "FROM search(2024_48, 2024_48, [130000]) ORDER BY keyword_id, shop_id, date, position LIMIT 5"
    {{duckdb}} "FROM search(2024_48, 2024_48, [130000], [131693, 131694]) ORDER BY keyword_id, shop_id, date, position LIMIT 5"

bench: release
    {{duckdb}} ".timer on" \
        -c "SELECT count(*) FROM product(2024_20, 2024_20, [1124000100000])" \
        -c "SELECT count(*) FROM search(2024_45, 2024_45, [98334])" \
        -c "SELECT count(*) FROM search(2026_01, 2026_04, [1, 2, 3])"
