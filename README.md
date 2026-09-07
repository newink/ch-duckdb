# ch_duckdb

DuckDB extension that connects to ClickHouse using the native client. It provides:
- `ATTACH ... TYPE clickhouse` to register a ClickHouse database inside DuckDB.
- `clickhouse_scan(...)` table function for ad-hoc reads without attaching.
- `clickhouse_query(alias, query)` table function that reuses credentials from a prior `ATTACH ... TYPE clickhouse`.

The current surface is read-only: DDL/DML against ClickHouse (CREATE/INSERT/UPDATE/DELETE) are not implemented yet.

## Requirements
- DuckDB 1.5.5 source as a submodule (`git submodule update --init --recursive`).
- CMake 3.12+ and a C++17 compiler.
- OpenSSL available to satisfy the ClickHouse client dependency (via system packages or vcpkg).
- `clickhouse-cpp` 2.6.2 is pinned in `third_party/`. The build enables TLS support.

## Building
```sh
git submodule update --init --recursive
# optional: speed up rebuilds
# GEN=ninja
# optional: use vcpkg toolchain for OpenSSL
# VCPKG_TOOLCHAIN_PATH=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
make
```

Outputs:
- `./build/release/duckdb` DuckDB shell. Load the extension explicitly as shown below.
- `./build/release/extension/ch_duckdb/ch_duckdb.duckdb_extension` loadable extension binary.

For an existing checkout with a DuckDB 1.4.2 build, use a separate build directory.
On macOS arm64 with Homebrew OpenSSL:

```sh
cmake -G Ninja -S duckdb -B build/duckdb-1.5.5 \
  -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_CXX_STANDARD=17 \
  -DEXTENSION_STATIC_BUILD=1 \
  -DDUCKDB_EXTENSION_CONFIGS="$PWD/extension_config.cmake" \
  -DOPENSSL_ROOT_DIR="$(brew --prefix openssl@3)" \
  -DUNITTEST_ROOT_DIRECTORY="$PWD" \
  -DENABLE_UNITTEST_CPP_TESTS=FALSE
cmake --build build/duckdb-1.5.5 \
  --target ch_duckdb_loadable_extension shell --parallel 8
./build/duckdb-1.5.5/duckdb -unsigned
```

Then load `build/duckdb-1.5.5/extension/ch_duckdb/ch_duckdb.duckdb_extension`.

## Usage
Start DuckDB with `-unsigned` for a local unsigned build, then load the extension:
```sql
LOAD 'build/release/extension/ch_duckdb/ch_duckdb.duckdb_extension';
-- or, when distributed: LOAD ch_duckdb;
```

### ATTACH ClickHouse
Attach a ClickHouse database to query its tables through DuckDB:
```sql
ATTACH DATABASE 'ch://localhost:9000/default' (TYPE clickhouse, USER 'default', PASSWORD 'secret');
-- Query tables using the chosen database alias (default: the path or the name you provide)
SELECT * FROM clickhouse.system_tables LIMIT 5;
```

Accepted ATTACH options (case-insensitive):
- path `ch://host:port/database` is parsed for host/port/database.
- `HOST`, `PORT`, `DATABASE`/`DB`, `USER`/`USERNAME`, `PASSWORD`, `SECURE` (boolean to enable TLS).

Connection is verified with `PING` during attach; credentials are redacted in logs.

### Table function for ad-hoc reads
Use `clickhouse_scan` without attaching:
```sql
SELECT * FROM clickhouse_scan(
  'SELECT number, text FROM system.numbers LIMIT 3',
  'localhost',       -- host (required)
  9000,              -- port (uint16)
  'default',         -- user
  NULL,              -- password (NULL to omit)
  'default',         -- database
  false              -- secure (true enables TLS with default CA paths)
);
```
Arguments after host can be NULL to fall back to defaults (port 9000, user "default", database "default", secure=false).

### Table function reusing an attached catalog
Use `clickhouse_query` to run ad-hoc queries against an attached ClickHouse alias without repeating credentials:
```sql
ATTACH DATABASE 'ch://localhost:9000/default' (TYPE clickhouse, USER 'default', PASSWORD 'secret') AS ch1;
SELECT * FROM clickhouse_query('ch1', 'SELECT number FROM system.numbers LIMIT 3');
```
The function signature is `clickhouse_query(alias, query)`; it looks up connection settings from the given attached
catalog name and fails with a descriptive error if the alias is unknown or not a ClickHouse attachment.

## Development
- Style: follow DuckDB conventions (namespaces in `duckdb`, headers in `src/include`, avoid `using namespace` in headers).
- The extension entry point lives in `src/ch_duckdb_extension.cpp`.
- Core ClickHouse plumbing: catalog/attach in `src/clickhouse_catalog.cpp`, transaction manager in `src/clickhouse_transaction_manager.cpp`, table function in `src/clickhouse_table_function.cpp`.
- Unimplemented operations (DDL/DML) currently raise `NotImplementedException`.

## Testing
SQLLogicTests in `test/sql/` cover validation and queries against a local ClickHouse.
Build the test runner and local extension repository, then start ClickHouse and run the tests:

```sh
cmake --build build/duckdb-1.5.5 \
  --target ch_duckdb_loadable_extension shell unittest duckdb_local_extension_repo --parallel 8
TEST_BUILD_DIR=build/duckdb-1.5.5 make test-clickhouse
make clickhouse-down
```

`make test-clickhouse` uses the standard `build/release` directory.
For the separate build above, run `TEST_BUILD_DIR=build/duckdb-1.5.5 make test-clickhouse`.
The test target does not rebuild. See [test/README.md](test/README.md) for connection settings and coverage.

## Contributing
1. Open an issue describing the change (bug, feature, or docs).
2. Fork/branch, run `make` (and tests when available).
3. Add focused SQLLogicTests for new behavior where possible.
4. Submit a PR with a short, imperative summary and commands/tests you ran.

## License
See `LICENSE` for details.
