# ch_duckdb

DuckDB extension that connects to ClickHouse using the native client. It provides:
- `ATTACH ... TYPE clickhouse` to register a ClickHouse database inside DuckDB.
- `clickhouse_scan(...)` table function for ad-hoc reads without attaching.
- `clickhouse_query(alias, query)` table function that reuses credentials from a prior `ATTACH ... TYPE clickhouse`.

The current surface is read-only: DDL/DML against ClickHouse (CREATE/INSERT/UPDATE/DELETE) are not implemented yet.

## Requirements
- DuckDB source as a submodule (`git submodule update --init --recursive`).
- CMake 3.5+ and a C++17 compiler.
- OpenSSL available to satisfy the ClickHouse client dependency (via system packages or vcpkg).
- `clickhouse-cpp` is vendored in `third_party/`.

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
- `./build/release/duckdb` DuckDB shell with the extension linked.
- `./build/release/extension/ch_duckdb/ch_duckdb.duckdb_extension` loadable extension binary.

## Usage
Load the extension (from the build tree or an installed copy):
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
SQLLogic tests will live under `test/sql/` (none are checked in yet). Run with:
```sh
make test       # release
# or
make test_debug # debug
```

## Contributing
1. Open an issue describing the change (bug, feature, or docs).
2. Fork/branch, run `make` (and tests when available).
3. Add focused SQLLogicTests for new behavior where possible.
4. Submit a PR with a short, imperative summary and commands/tests you ran.

## License
See `LICENSE` for details.
