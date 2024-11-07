FROM ubuntu:24.04

RUN apt update && apt install -y build-essential cmake git curl zip unzip tar

RUN git clone https://github.com/Microsoft/vcpkg.git /opt/vcpkg
RUN /opt/vcpkg/bootstrap-vcpkg.sh 
ENV PATH=$PATH:/opt/vcpkg

WORKDIR /app
COPY duckdb src test ./
RUN make

FROM google/cloud-sdk:slim

WORKDIR /app
COPY --from=0 /app/build/build/release/repository/ /app
RUN gsutil -m rsync -r . gs://di_duckdb_extension
