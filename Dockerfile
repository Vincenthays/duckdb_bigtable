FROM ubuntu:24.04

RUN apt update && apt install -y build-essential cmake git curl zip unzip tar pkg-config

WORKDIR /opt
RUN git clone https://github.com/Microsoft/vcpkg.git
RUN ./vcpkg/bootstrap-vcpkg.sh 
ENV VCPKG_TOOLCHAIN_PATH=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake

WORKDIR /app
COPY . ./
RUN make

FROM google/cloud-sdk:slim

WORKDIR /app
COPY --from=0 /app/build/release/extension/bigtable2/bigtable2.duckdb_extension .
RUN gzip bigtable2.duckdb_extension
CMD gsutil cp bigtable2.duckdb_extension.gz gs://di_duckdb_extension/v1.1.3/linux_amd64/bigtable2.duckdb_extension.gz
