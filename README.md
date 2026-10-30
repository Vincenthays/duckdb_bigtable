# bigtable2 (DuckDB C API v2 extension)

The `product` and `search` table functions from `bigtable2-duckdb`, rebuilt on DuckDB 2.0's C API v2
(through the C++ wrapper `duckdb_cpp.hpp`, vendored in `third_party/duckdb`). It is built standalone and does
not need the DuckDB source tree.

The C API v2 is still unstable, so the extension is pinned to DuckDB **v2.0.0** (`-DDUCKDB_VERSION=...` to change).

## Build & install

```sh
make install          # builds build/bigtable2.duckdb_extension and installs it into ~/.duckdb/extensions
```

Dependencies come from vcpkg in manifest mode (`vcpkg.json`, `VCPKG_ROOT` defaults to `~/vcpkg`, at release 2026.10.16):
google-cloud-cpp 2.37.0#1, gRPC 1.81.1, protobuf 6.33.4, abseil 20260107.1. fast_float 8.3.2 is vendored.
Built as C++26.

## Use

The extension is unsigned, so start DuckDB with `-unsigned` (or `SET allow_unsigned_extensions = true`):

```sql
LOAD bigtable2;
SELECT * FROM product(202601, 202601, [1250000644300]);           -- optional 4th arg: shop_ids BIGINT[]
SELECT * FROM search(202601, 202604, [1, 2, 3], [115914]);
```

## Just recipes

| Recipe | What it does |
|---|---|
| `just install [2.0.0]` | Build for this machine and install into local DuckDB |
| `just test` / `just bench` | Run sample queries / timings |
| `just duckdb-headers 2.0.0` | Re-vendor the C API v2 headers for a DuckDB release |
| `just deploy 2.0.0` | macOS: build + upload `osx_arm64`. Linux: build `linux_amd64` and `linux_amd64_musl` in Docker + upload |
| `just build-linux` | Build both Linux variants via Docker into `dist/` (works on macOS through emulation) |

Artifacts go to `gs://di_duckdb_extension/v<version>/<platform>/bigtable2.duckdb_extension.gz`.
