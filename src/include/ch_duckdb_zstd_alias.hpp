#pragma once

#ifdef DUCKDB_BUILD_LIBRARY
// DuckDB namespaces its bundled zstd symbols to duckdb_zstd; import the ones used by clickhouse-cpp.
#include <zstd.h>
using duckdb_zstd::ZSTD_compress;
using duckdb_zstd::ZSTD_compressBound;
using duckdb_zstd::ZSTD_decompress;
using duckdb_zstd::ZSTD_fast;
using duckdb_zstd::ZSTD_getErrorName;
using duckdb_zstd::ZSTD_isError;
#endif
