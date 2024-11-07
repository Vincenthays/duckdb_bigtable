FROM ubuntu:24.04

RUN apt update && apt install -y build-essential cmake git curl zip unzip tar pkg-config

WORKDIR /opt
RUN git clone https://github.com/Microsoft/vcpkg.git
RUN ./vcpkg/bootstrap-vcpkg.sh 
ENV VCPKG_TOOLCHAIN_PATH=/opt/vcpkg/scripts/buildsystems/vcpkg.cmake

WORKDIR /app
COPY . ./
RUN make

