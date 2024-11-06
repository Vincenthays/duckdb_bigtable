FROM ubuntu:24.04

RUN apt update && apt install -y build-essential cmake git curl zip unzip tar

RUN git clone https://github.com/Microsoft/vcpkg.git /opt/vcpkg
RUN /opt/vcpkg/bootstrap-vcpkg.sh 
ENV PATH=$PATH:/opt/vcpkg

WORKDIR /app
COPY duckdb src test ./
RUN make

# gs://<s3_bucket>/<ext_name>/<extension_version>/<duckdb_version>/<architecture>/<ext_name>.duckdb_extension
