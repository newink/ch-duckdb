# ClickHouse integration

The extension exposes a `clickhouse_scan` table function and an ATTACH helper that rely on the ClickHouse C++ client.

## Attaching ClickHouse

```sql
ATTACH 'clickhouse' (
    TYPE clickhouse,
    HOST 'localhost',
    PORT 9000,
    DATABASE 'default',
    USER 'default',
    PASSWORD '',
    SECURE false
);
```

After attaching, you can automatically expose tables from a ClickHouse database as DuckDB views using the helper function `clickhouse_attach`. It creates a schema of views that route back through the ClickHouse client so you can query them like native tables:

```sql
-- create a schema called "ch" with views for each ClickHouse table in the "default" database
SELECT * FROM clickhouse_attach('ch', host='localhost', port=9000, database='default');

-- query a table as a normal relation
SELECT * FROM ch.some_table LIMIT 10;
```

You can still issue ad-hoc queries with the `clickhouse_scan` table function by providing a SQL query and matching connection options:

```sql
SELECT *
FROM clickhouse_scan(
    'SELECT number, toString(number) AS as_text FROM system.numbers LIMIT 10',
    host='localhost',
    port=9000,
    database='default',
    user='default',
    password='',
    secure=false
);
```

Both entry points validate the connection by pinging ClickHouse and will report connection errors or schema inference problems when the query cannot be executed.
