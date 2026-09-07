# Testing the extension

The root `compose.yaml` starts ClickHouse 26.3.32.14 for SQLLogicTests.
Docker Compose, a built DuckDB shell, the loadable extension and the `unittest` runner are required.

After configuring the build as described in the root README:

```sh
cmake --build build/duckdb-1.5.5 \
  --target ch_duckdb_loadable_extension shell unittest duckdb_local_extension_repo --parallel 8
make test-clickhouse
```

`make test-clickhouse` installs the current binary into `build/duckdb-1.5.5/test_extensions`,
starts the container, waits for the fixture data and runs `test/sql/*`. It does not rebuild.
Set `TEST_BUILD_DIR=build/release` to use a different build.
The container stays running after the tests.

```sh
make clickhouse-up
docker compose ps
make clickhouse-down
```

The test database and logs use tmpfs. Stopping the container discards them.
The initialization script recreates the sample table when the container starts with empty storage.

| Setting | Value |
| --- | --- |
| Native protocol | `127.0.0.1:19000` |
| HTTP | `http://127.0.0.1:18123` |
| Database and user | `ch_duckdb_test` |
| Password | `test_password` |

These credentials are only for the local test container. Both ports bind to localhost.
To change the native port, run `CH_TEST_PORT=19001 make test-clickhouse`.

`functions.test` checks registration, alias validation and the port range without a server.
`clickhouse.test` requires `CH_TEST_ENABLED=1`, which the Make target supplies. It checks:

- Direct scans and queries through an attached catalog, including detach and error handling.
- Unicode, empty and long strings, NULL, lists, LowCardinality strings and tuples.
- Dates, timestamps at several precisions, dates before 1970 and UUIDs.
- 10,000 rows across ClickHouse blocks and DuckDB chunks, including reused string and list vectors.

This fixture uses the plain native protocol. It does not test TLS.
