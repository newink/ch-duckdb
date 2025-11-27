#include "clickhouse_transaction_manager.hpp"

#include "duckdb/common/mutex.hpp"
#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

ClickhouseTransactionManager::ClickhouseTransactionManager(AttachedDatabase &db_p, Catalog &catalog_p)
    : TransactionManager(db_p), catalog(catalog_p), transaction_lock(), transactions() {
}

ClickhouseTransactionManager::~ClickhouseTransactionManager() {
}

Transaction &
ClickhouseTransactionManager::StartTransaction(ClientContext &context) {
	auto transaction = make_uniq<ClickhouseTransaction>(*this, context);
	auto &result = *transaction;
	lock_guard<mutex> l(transaction_lock);
	transactions[&result] = std::move(transaction);
	return result;
}

ErrorData
ClickhouseTransactionManager::CommitTransaction(ClientContext &, Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(&transaction);
	return ErrorData();
}

void
ClickhouseTransactionManager::RollbackTransaction(Transaction &transaction) {
	lock_guard<mutex> l(transaction_lock);
	transactions.erase(&transaction);
}

void
ClickhouseTransactionManager::Checkpoint(ClientContext &, bool) {
	// ClickHouse connections are remote; checkpointing is a no-op for now.
}

} // namespace duckdb
