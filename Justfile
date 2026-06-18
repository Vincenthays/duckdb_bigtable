[macos]
deploy DUCKDB_VERSION:
    #!/usr/bin/env sh
    cd duckdb
    git checkout "tags/v{{DUCKDB_VERSION}}" || exit 1
    cd ..

    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make
    cat build/release/extension/bigtable2/bigtable2.duckdb_extension | gzip | gsutil cp - gs://di_duckdb_extension/v{{DUCKDB_VERSION}}/osx_arm64/bigtable2.duckdb_extension.gz

[linux]
deploy DUCKDB_VERSION:
    #!/usr/bin/env sh
    git reset --hard origin
    git pull
    git submodule update --init --recursive
    git fetch --tags --recurse-submodules

    cd duckdb
    git checkout "tags/v{{DUCKDB_VERSION}}" || exit 1
    cd ..

    docker build -f Dockerfile_linux_amd64_musl -t duckdb_extension_linux_amd64_musl .
    docker run -i -v /home/dataimpact/gs.json:/app/gs.json duckdb_extension_linux_amd64_musl bash <<EOF
        gcloud auth activate-service-account --key-file /app/gs.json
        gsutil cp bigtable2.duckdb_extension.gz gs://di_duckdb_extension/v{{DUCKDB_VERSION}}/linux_amd64_musl/bigtable2.duckdb_extension.gz
    EOF

    docker build -f Dockerfile_linux_amd64 -t duckdb_extension_linux_amd64 .
    docker run -i -v /home/dataimpact/gs.json:/app/gs.json duckdb_extension_linux_amd64 bash <<EOF
        gcloud auth activate-service-account --key-file /app/gs.json
        gsutil cp bigtable2.duckdb_extension.gz gs://di_duckdb_extension/v{{DUCKDB_VERSION}}/linux_amd64/bigtable2.duckdb_extension.gz
    EOF

debug:
    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug

release:
    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make

test: test_product test_search
    
test_product: debug
    ./build/debug/duckdb --init /dev/null -c "FROM product(2024_20, 2024_20, [1124000100000])"
    ./build/debug/duckdb --init /dev/null -c "FROM product(2024_20, 2024_20, [1124000100000], [41188])"

test_search: debug
    ./build/debug/duckdb --init /dev/null -c "FROM search(2024_48, 2024_48, [130000]) ORDER BY keyword_id, shop_id, date, position"
    ./build/debug/duckdb --init /dev/null -c "FROM search(2024_48, 2024_48, [130000], [131693]) ORDER BY keyword_id, shop_id, date, position"

bench: release
    ./build/release/duckdb --init /dev/null -c ".timer on" \
        -c "FROM product(2024_20, 2024_20, [1124000100000])" \
        -c "FROM search(2024_45, 2024_45, [98334])"

test_filter_pushdown: debug
    ./build/debug/duckdb --init /dev/null -c "SELECT pe_id, price FROM product(2024_20, 2024_20, [1124000100000])"
    ./build/debug/duckdb --init /dev/null -c "FROM product(2024_20, 2024_20, [1124000100000])"
    ./build/debug/duckdb --init /dev/null -c "SELECT pe_id FROM product(2024_20, 2024_20, [1124000100000])"
