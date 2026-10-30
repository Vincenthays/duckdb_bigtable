#include "duckdb_cpp_extension.hpp"
#include "common.hpp"

using namespace duckdb::cxx;

DUCKDB_CPP_EXTENSION_ENTRYPOINT(Extension &extension, Context &context) {
	auto &factory = context.GetFactory();
	bigtable2::RegisterProduct(extension, factory);
	bigtable2::RegisterSearch(extension, factory);
}
