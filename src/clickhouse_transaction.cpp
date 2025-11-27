#include "clickhouse_transaction.hpp"

namespace duckdb {

ClickhouseTransaction::ClickhouseTransaction(TransactionManager &manager, ClientContext &context)
    : Transaction(manager, context) {
}

ClickhouseTransaction::~ClickhouseTransaction() = default;

} // namespace duckdb
