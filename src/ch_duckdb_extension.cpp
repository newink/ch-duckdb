#define DUCKDB_EXTENSION_MAIN

#include "ch_duckdb_extension.hpp"
#include "clickhouse_storage_extension.hpp"
#include "clickhouse_table_function.hpp"
#include "clickhouse_scan_table_function.hpp"
#include "duckdb.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

static void LoadInternal(ExtensionLoader &loader) {
	// Register ClickHouse storage extension so ATTACH ... TYPE clickhouse is available
	auto &db = loader.GetDatabaseInstance();
	auto &config = DBConfig::GetConfig(db);
	if (config.storage_extensions.find("clickhouse") == config.storage_extensions.end()) {
		config.storage_extensions["clickhouse"] = make_uniq<ClickhouseStorageExtension>();
	}

	// Register table functions for ad-hoc reads and attached database queries
	loader.RegisterFunction(ClickhouseQueryFunction::GetFunction());
	loader.RegisterFunction(ClickhouseScanFunction::GetFunction());
}

void ChDuckdbExtension::Load(ExtensionLoader &loader) {
	LoadInternal(loader);
}
std::string ChDuckdbExtension::Name() {
	return "ch_duckdb";
}

std::string ChDuckdbExtension::Version() const {
#ifdef EXT_VERSION_CH_DUCKDB
	return EXT_VERSION_CH_DUCKDB;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_CPP_EXTENSION_ENTRY(ch_duckdb, loader) {
	duckdb::LoadInternal(loader);
}
}
