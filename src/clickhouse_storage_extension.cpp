#include "clickhouse_storage_extension.hpp"

#include "clickhouse_catalog.hpp"
#include "clickhouse_transaction_manager.hpp"

namespace duckdb {

namespace {

unique_ptr<TransactionManager>
CreateClickhouseTransactionManager(optional_ptr<StorageExtensionInfo>, AttachedDatabase &db, Catalog &catalog) {
	return make_uniq<ClickhouseTransactionManager>(db, catalog);
}

} // namespace

ClickhouseStorageExtension::ClickhouseStorageExtension() {
	attach = ClickhouseCatalog::Attach;
	create_transaction_manager = CreateClickhouseTransactionManager;
}

} // namespace duckdb
