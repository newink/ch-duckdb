CREATE TABLE ch_duckdb_test.sample
(
    id UInt64,
    name String,
    description String,
    optional_value Nullable(Int32),
    items Array(Int32),
    category LowCardinality(String)
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO ch_duckdb_test.sample VALUES
    (1, 'Привет', 'a description longer than twelve bytes', NULL, [1, 2, 3], 'first'),
    (2, '', 'another description longer than twelve bytes', -7, [], 'second');
