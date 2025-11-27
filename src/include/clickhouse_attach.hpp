#pragma once

#include "duckdb.hpp"
#include <memory>
#include <string>

#include <clickhouse/client.h>

namespace duckdb {

struct ClickHouseConnectionOptions {
        std::string host;
        int port = 9000;
        std::string database;
        std::string user;
        std::string password;
        bool secure = false;
};

class ClickHouseAttachFunction {
public:
        explicit ClickHouseAttachFunction(ClickHouseConnectionOptions options);

        //! Build a ClickHouse client using the supplied options. Throws an exception on failure.
        std::shared_ptr<clickhouse::Client> CreateClient();

        const ClickHouseConnectionOptions &GetOptions() const { return options; }

private:
        ClickHouseConnectionOptions options;
};

//! Register the ClickHouse table/attach functions with the DuckDB extension loader.
void RegisterClickHouseAttachFunctions(ExtensionLoader &loader);

} // namespace duckdb

