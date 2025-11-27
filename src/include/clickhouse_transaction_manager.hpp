#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/unordered_map.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "clickhouse_transaction.hpp"

namespace duckdb {

class ClickhouseTransactionManager : public TransactionManager {
public:
	ClickhouseTransactionManager(AttachedDatabase &db, Catalog &catalog);
	~ClickhouseTransactionManager() override;

	Transaction &StartTransaction(ClientContext &context) override;
	ErrorData CommitTransaction(ClientContext &context, Transaction &transaction) override;
	void RollbackTransaction(Transaction &transaction) override;
	void Checkpoint(ClientContext &context, bool force = false) override;

	bool IsDuckTransactionManager() override {
		return false;
	}

private:
	[[maybe_unused]] Catalog &catalog;
	mutex transaction_lock;
	unordered_map<Transaction *, unique_ptr<ClickhouseTransaction>> transactions;
};

} // namespace duckdb
