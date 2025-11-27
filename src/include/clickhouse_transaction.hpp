#pragma once

#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

class ClickhouseTransaction : public Transaction {
public:
	ClickhouseTransaction(TransactionManager &manager, ClientContext &context);
	~ClickhouseTransaction() override;

	bool IsDuckTransaction() const override {
		return false;
	}
};

} // namespace duckdb
