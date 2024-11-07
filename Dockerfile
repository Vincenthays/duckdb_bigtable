FROM ubuntu:24.04

RUN apt update && apt install -y build-essential cmake git curl zip unzip tar

WORKDIR /opt
RUN git clone https://github.com/Microsoft/vcpkg.git
RUN ./vcpkg/bootstrap-vcpkg.sh 
ENV VCPKG_TOOLCHAIN_PATH=$PWD/vcpkg/scripts/buildsystems/vcpkg.cmake

WORKDIR /app
COPY . ./
RUN make

FROM google/cloud-sdk:slim

WORKDIR /app
COPY --from=0 /app/build/release/repository ./
RUN gsutil -m rsync -r . gs://di_duckdb_extension
