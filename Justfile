[macos]
deploy:
    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make
    @cat build/release/extension/bigtable2/bigtable2.duckdb_extension | gzip | gsutil cp - gs://di_duckdb_extension/v1.1.3/osx_arm64/bigtable2.duckdb_extension.gz

[linux]
deploy:
    #!/usr/bin/env sh
    git reset --hard origin
    git pull

    docker build -f Dockerfile_linux_amd64 -t duckdb_extension_linux_amd64 . &
    docker build -f Dockerfile_linux_amd64_gcc4 -t duckdb_extension_linux_amd64_gcc4 . &
    wait

    docker run -i -v /home/dataimpact/gs.json:/app/gs.json duckdb_extension_linux_amd64 bash <<EOF
        gcloud auth activate-service-account --key-file /app/gs.json
        gsutil cp bigtable2.duckdb_extension.gz gs://di_duckdb_extension/v1.1.3/linux_amd64/bigtable2.duckdb_extension.gz
    EOF

    docker run -i -v /home/dataimpact/gs.json:/app/gs.json duckdb_extension_linux_amd64_gcc4 bash <<EOF
        gcloud auth activate-service-account --key-file /app/gs.json
        gsutil cp bigtable2.duckdb_extension.gz gs://di_duckdb_extension/v1.1.3/linux_amd64_gcc4/bigtable2.duckdb_extension.gz
    EOF

debug:
    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make debug

release:
    VCPKG_TOOLCHAIN_PATH=$HOME/vcpkg/scripts/buildsystems/vcpkg.cmake GEN=ninja make

test: test_product test_search
    
test_product: debug
    ./build/debug/duckdb --init '' -c "FROM product(2024_20, 2024_20, [1124000100000])"

test_search: debug
    ./build/debug/duckdb --init '' -c "FROM search(2024_48, 2024_48, [130000])"

bench: release
    time ./build/release/duckdb --init '' -c "FROM search(2024_45, 2024_45, [98334])"

test_filter_pushdown:
    ./build/debug/duckdb --init '' -c "SELECT pe_id FROM product(2024_20, 2024_20, [1124000100000])"
