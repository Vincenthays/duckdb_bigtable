[macos]
deploy:
    @make
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
