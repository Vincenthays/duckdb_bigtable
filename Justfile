[macos]
upload:
    @cat build/release/extension/bigtable2/bigtable2.duckdb_extension | gzip | gsutil cp - gs://di_duckdb_extension/v1.1.3/osx_arm64/bigtable2.duckdb_extension.gz

[linux]
upload:
    @git pull
    @docker build . -t duckdb_extension 
    @echo "gcloud auth activate-service-account --key-file /app/gs.json && gsutil cp bigtable2.duckdb_extension.gz gs://di_duckdb_extension/v1.1.3/linux_amd64/bigtable2.duckdb_extension.gz" | docker run -it -v /home/dataimpact/gs.json:/app/gs.json duckdb_extension bash
