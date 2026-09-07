PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=ch_duckdb
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

TEST_BUILD_DIR ?= build/release

.PHONY: clickhouse-up clickhouse-down test-clickhouse
clickhouse-up:
	docker compose up -d --wait --wait-timeout 120

clickhouse-down:
	docker compose down

# Run against the selected build without triggering a rebuild.
test-clickhouse:
	@test -x "$(TEST_BUILD_DIR)/test/unittest" && test -x "$(TEST_BUILD_DIR)/duckdb" && \
		test -f "$(TEST_BUILD_DIR)/extension/ch_duckdb/ch_duckdb.duckdb_extension" || \
		{ echo "Missing test build in $(TEST_BUILD_DIR). Run make, or set TEST_BUILD_DIR to a complete build." >&2; exit 1; }
	"$(TEST_BUILD_DIR)/duckdb" -unsigned -c "SET extension_directory='$(abspath $(TEST_BUILD_DIR))/test_extensions'; FORCE INSTALL '$(abspath $(TEST_BUILD_DIR))/extension/ch_duckdb/ch_duckdb.duckdb_extension'; LOAD ch_duckdb;"
	$(MAKE) clickhouse-up
	CH_TEST_ENABLED=1 DUCKDB_TEST_AUTOLOADING=available \
		LOCAL_EXTENSION_REPO="$(abspath $(TEST_BUILD_DIR))/repository" \
		DUCKDB_TEST_SETTINGS="[{name: extension_directory, value: '$(abspath $(TEST_BUILD_DIR))/test_extensions'}]" \
		"$(TEST_BUILD_DIR)/test/unittest" "test/sql/*"
