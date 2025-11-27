#include "clickhouse_attach.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/identifier.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension_util.hpp"

#include <clickhouse/columns/date.h>
#include <clickhouse/columns/datetime.h>
#include <clickhouse/columns/decimal.h>
#include <clickhouse/columns/float32.h>
#include <clickhouse/columns/float64.h>
#include <clickhouse/columns/int16.h>
#include <clickhouse/columns/int32.h>
#include <clickhouse/columns/int64.h>
#include <clickhouse/columns/int8.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uint16.h>
#include <clickhouse/columns/uint32.h>
#include <clickhouse/columns/uint64.h>
#include <clickhouse/columns/uint8.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/client.h>

namespace duckdb {

ClickHouseAttachFunction::ClickHouseAttachFunction(ClickHouseConnectionOptions options_p)
    : options(std::move(options_p)) {
}

static clickhouse::ClientOptions BuildClientOptions(const ClickHouseConnectionOptions &options) {
        clickhouse::ClientOptions client_options;
        client_options.SetHost(options.host);
        client_options.SetPort(options.port);
        if (!options.database.empty()) {
                client_options.SetDefaultDatabase(options.database);
        }
        if (!options.user.empty()) {
                client_options.SetUser(options.user);
        }
        if (!options.password.empty()) {
                client_options.SetPassword(options.password);
        }
        client_options.SetSecure(options.secure);
        return client_options;
}

static std::string EscapeClickHouseString(const std::string &value) {
        auto escaped = value;
        StringUtil::Replace(escaped, "'", "''");
        return escaped;
}

std::shared_ptr<clickhouse::Client> ClickHouseAttachFunction::CreateClient() {
        auto client_options = BuildClientOptions(options);
        try {
                auto client = std::make_shared<clickhouse::Client>(client_options);
                // ping to ensure connectivity
                client->Ping();
                return client;
        } catch (const std::exception &ex) {
                throw InvalidInputException("Failed to connect to ClickHouse at %s:%d: %s", options.host, options.port,
                                            ex.what());
        }
}

struct ClickHouseScanData : public TableFunctionData {
        ClickHouseConnectionOptions options;
        std::string query;
        std::shared_ptr<clickhouse::Client> client;
        std::vector<std::string> names;
        std::vector<LogicalType> types;
        std::vector<std::vector<Value>> rows;
};

class ClickHouseScanState : public GlobalTableFunctionState {
public:
        explicit ClickHouseScanState(idx_t total_rows) : offset(0), total_rows(total_rows) {
        }

        idx_t offset;
        idx_t total_rows;

        idx_t MaxThreads() const override {
                return 1;
        }
};

static LogicalType MapClickHouseType(const clickhouse::Type::Code &code) {
        switch (code) {
        case clickhouse::Type::Code::Int8:
                return LogicalType::TINYINT;
        case clickhouse::Type::Code::Int16:
                return LogicalType::SMALLINT;
        case clickhouse::Type::Code::Int32:
                return LogicalType::INTEGER;
        case clickhouse::Type::Code::Int64:
                return LogicalType::BIGINT;
        case clickhouse::Type::Code::UInt8:
                return LogicalType::UTINYINT;
        case clickhouse::Type::Code::UInt16:
                return LogicalType::USMALLINT;
        case clickhouse::Type::Code::UInt32:
                return LogicalType::UINTEGER;
        case clickhouse::Type::Code::UInt64:
                return LogicalType::UBIGINT;
        case clickhouse::Type::Code::Float32:
                return LogicalType::FLOAT;
        case clickhouse::Type::Code::Float64:
                return LogicalType::DOUBLE;
        case clickhouse::Type::Code::String:
        case clickhouse::Type::Code::FixedString:
                return LogicalType::VARCHAR;
        default:
                return LogicalType::VARCHAR;
        }
}

static Value ReadClickHouseValue(const clickhouse::ColumnRef &column, const clickhouse::Type::Code &code, idx_t row) {
        switch (code) {
        case clickhouse::Type::Code::Int8:
                return Value::TINYINT(column->As<clickhouse::ColumnInt8>()->At(row));
        case clickhouse::Type::Code::Int16:
                return Value::SMALLINT(column->As<clickhouse::ColumnInt16>()->At(row));
        case clickhouse::Type::Code::Int32:
                return Value::INTEGER(column->As<clickhouse::ColumnInt32>()->At(row));
        case clickhouse::Type::Code::Int64:
                return Value::BIGINT(column->As<clickhouse::ColumnInt64>()->At(row));
        case clickhouse::Type::Code::UInt8:
                return Value::UTINYINT(column->As<clickhouse::ColumnUInt8>()->At(row));
        case clickhouse::Type::Code::UInt16:
                return Value::USMALLINT(column->As<clickhouse::ColumnUInt16>()->At(row));
        case clickhouse::Type::Code::UInt32:
                return Value::UINTEGER(column->As<clickhouse::ColumnUInt32>()->At(row));
        case clickhouse::Type::Code::UInt64:
                return Value::UBIGINT(column->As<clickhouse::ColumnUInt64>()->At(row));
        case clickhouse::Type::Code::Float32:
                return Value::FLOAT(column->As<clickhouse::ColumnFloat32>()->At(row));
        case clickhouse::Type::Code::Float64:
                return Value::DOUBLE(column->As<clickhouse::ColumnFloat64>()->At(row));
        case clickhouse::Type::Code::String:
        case clickhouse::Type::Code::FixedString:
                return Value(column->As<clickhouse::ColumnString>()->At(row));
        default:
                return Value();
        }
}

static std::unique_ptr<FunctionData> ClickHouseBind(ClientContext &context, TableFunctionBindInput &input,
                                                   std::vector<LogicalType> &return_types, std::vector<std::string> &names) {
        if (input.inputs.empty()) {
                throw BinderException("clickhouse_scan requires a query string as the first argument");
        }

        ClickHouseConnectionOptions options;
        options.host = input.named_parameters.count("host") ? StringValue::Get(input.named_parameters.at("host")) : "localhost";
        options.port = input.named_parameters.count("port") ? IntegerValue::Get(input.named_parameters.at("port")) : 9000;
        options.database = input.named_parameters.count("database") ? StringValue::Get(input.named_parameters.at("database")) : "";
        options.user = input.named_parameters.count("user") ? StringValue::Get(input.named_parameters.at("user")) : "";
        options.password = input.named_parameters.count("password") ? StringValue::Get(input.named_parameters.at("password")) : "";
        options.secure = input.named_parameters.count("secure") ? BooleanValue::Get(input.named_parameters.at("secure")) : false;

        auto query = StringValue::Get(input.inputs[0]);

        auto bind_data = std::make_unique<ClickHouseScanData>();
        bind_data->options = options;
        bind_data->query = query;

        ClickHouseAttachFunction attach_function(options);
        bind_data->client = attach_function.CreateClient();

        try {
                bind_data->client->Select(query, [&](const clickhouse::Block &block) {
                        if (block.GetRowCount() == 0 && block.GetColumnCount() == 0) {
                                return;
                        }
                        if (names.empty()) {
                                for (size_t col_idx = 0; col_idx < block.GetColumnCount(); col_idx++) {
                                        auto type_code = block.GetColumn(col_idx)->Type()->GetCode();
                                        names.emplace_back(block.GetColumnName(col_idx));
                                        auto mapped_type = MapClickHouseType(type_code);
                                        return_types.emplace_back(mapped_type);
                                        bind_data->types.emplace_back(mapped_type);
                                        bind_data->names.emplace_back(block.GetColumnName(col_idx));
                                }
                        }

                        auto row_count = block.GetRowCount();
                        for (size_t row_idx = 0; row_idx < row_count; row_idx++) {
                                std::vector<Value> row_values;
                                row_values.reserve(block.GetColumnCount());
                                for (size_t col_idx = 0; col_idx < block.GetColumnCount(); col_idx++) {
                                        auto type_code = block.GetColumn(col_idx)->Type()->GetCode();
                                        row_values.push_back(ReadClickHouseValue(block.GetColumn(col_idx), type_code, row_idx));
                                }
                                bind_data->rows.push_back(std::move(row_values));
                        }
                });
        } catch (const std::exception &ex) {
                throw BinderException("Failed to execute ClickHouse query: %s", ex.what());
        }

        if (names.empty()) {
                throw BinderException("Unable to determine ClickHouse result schema from query result");
        }

        return std::move(bind_data);
}

static std::unique_ptr<GlobalTableFunctionState> ClickHouseInitGlobal(ClientContext &context,
                                                                     TableFunctionInitInput &input) {
        auto &bind_data = input.bind_data->Cast<ClickHouseScanData>();
        return std::make_unique<ClickHouseScanState>(bind_data.rows.size());
}

static void ClickHouseFunction(ClientContext &context, TableFunctionInput &data, DataChunk &output) {
        auto &bind_data = data.bind_data->Cast<ClickHouseScanData>();
        auto &state = data.global_state->Cast<ClickHouseScanState>();

        idx_t output_count = 0;
        while (state.offset < state.total_rows && output_count < STANDARD_VECTOR_SIZE) {
                auto &row = bind_data.rows[state.offset];
                for (idx_t col_idx = 0; col_idx < row.size(); col_idx++) {
                        output.SetValue(col_idx, output_count, row[col_idx]);
                }
                state.offset++;
                output_count++;
        }
        output.SetCardinality(output_count);
}

static TableFunction GetClickHouseScanFunction() {
        TableFunction function("clickhouse_scan", {LogicalType::VARCHAR}, ClickHouseFunction, ClickHouseBind,
                               ClickHouseInitGlobal);
        function.named_parameters["host"] = LogicalTypeId::VARCHAR;
        function.named_parameters["port"] = LogicalTypeId::INTEGER;
        function.named_parameters["database"] = LogicalTypeId::VARCHAR;
        function.named_parameters["user"] = LogicalTypeId::VARCHAR;
        function.named_parameters["password"] = LogicalTypeId::VARCHAR;
        function.named_parameters["secure"] = LogicalTypeId::BOOLEAN;
        return function;
}

void RegisterClickHouseAttachFunctions(ExtensionLoader &loader) {
        ExtensionUtil::RegisterFunction(loader, GetClickHouseScanFunction());

        // Register a helper that discovers ClickHouse tables and materializes them as DuckDB views so they can be
        // queried without explicitly calling the scan function.
        auto clickhouse_attach_function = TableFunction(
            "clickhouse_attach", {LogicalType::VARCHAR}, ClickHouseFunction,
            [](ClientContext &context, TableFunctionBindInput &input, std::vector<LogicalType> &return_types,
               std::vector<std::string> &names) -> std::unique_ptr<FunctionData> {
                    if (input.inputs.empty()) {
                            throw BinderException("clickhouse_attach requires a schema name as its first argument");
                    }

                    ClickHouseConnectionOptions options;
                    options.host = input.named_parameters.count("host") ? StringValue::Get(input.named_parameters.at("host")) : "localhost";
                    options.port = input.named_parameters.count("port") ? IntegerValue::Get(input.named_parameters.at("port")) : 9000;
                    options.database = input.named_parameters.count("database") ? StringValue::Get(input.named_parameters.at("database")) : "";
                    options.user = input.named_parameters.count("user") ? StringValue::Get(input.named_parameters.at("user")) : "";
                    options.password = input.named_parameters.count("password") ? StringValue::Get(input.named_parameters.at("password")) : "";
                    options.secure = input.named_parameters.count("secure") ? BooleanValue::Get(input.named_parameters.at("secure")) : false;

                    auto schema_name = StringValue::Get(input.inputs[0]);
                    if (options.database.empty()) {
                            options.database = schema_name;
                    }

                    ClickHouseAttachFunction attach_function(options);
                    auto client = attach_function.CreateClient();

                    // Fetch all tables in the target database.
                    std::vector<std::string> table_names;
                    const auto escaped_db = EscapeClickHouseString(options.database);
                    const auto list_query = "SELECT name FROM system.tables WHERE database='" + escaped_db + "'";
                    try {
                            client->Select(list_query, [&](const clickhouse::Block &block) {
                                    for (size_t row_idx = 0; row_idx < block.GetRowCount(); row_idx++) {
                                            table_names.emplace_back(block[0]->As<clickhouse::ColumnString>()->At(row_idx));
                                    }
                            });
                    } catch (const std::exception &ex) {
                            throw BinderException("Failed to enumerate ClickHouse tables: %s", ex.what());
                    }

                    if (table_names.empty()) {
                            throw BinderException("No tables found in ClickHouse database '%s'", options.database);
                    }

                    Connection con(context.db);
                    con.Query(StringUtil::Format("CREATE SCHEMA IF NOT EXISTS %s", Identifier(schema_name)));
                    for (auto &table_name : table_names) {
                            auto view_sql = StringUtil::Format(
                                "CREATE OR REPLACE VIEW %s.%s AS SELECT * FROM clickhouse_scan('SELECT * FROM %s.%s', "
                                "host=%s, port=%d, database=%s, user=%s, password=%s, secure=%s)",
                                Identifier(schema_name), Identifier(table_name), Identifier(options.database), Identifier(table_name),
                                Value(options.host).ToSQLString(), options.port, Value(options.database).ToSQLString(),
                                Value(options.user).ToSQLString(), Value(options.password).ToSQLString(), options.secure ? "true" : "false");
                            auto result = con.Query(view_sql);
                            if (result->HasError()) {
                                    throw BinderException("Failed to register ClickHouse table %s: %s", table_name.c_str(),
                                                          result->GetError().c_str());
                            }
                    }

                    names.emplace_back("table_name");
                    return_types.emplace_back(LogicalType::VARCHAR);

                    auto bind_data = std::make_unique<ClickHouseScanData>();
                    bind_data->rows.reserve(table_names.size());
                    for (auto &name : table_names) {
                            bind_data->rows.push_back({Value(name)});
                    }
                    bind_data->names = names;
                    bind_data->types = return_types;
                    bind_data->query = "";
                    bind_data->options = options;
                    return std::move(bind_data);
            },
            ClickHouseInitGlobal);

        clickhouse_attach_function.named_parameters["host"] = LogicalTypeId::VARCHAR;
        clickhouse_attach_function.named_parameters["port"] = LogicalTypeId::INTEGER;
        clickhouse_attach_function.named_parameters["database"] = LogicalTypeId::VARCHAR;
        clickhouse_attach_function.named_parameters["user"] = LogicalTypeId::VARCHAR;
        clickhouse_attach_function.named_parameters["password"] = LogicalTypeId::VARCHAR;
        clickhouse_attach_function.named_parameters["secure"] = LogicalTypeId::BOOLEAN;
        ExtensionUtil::RegisterFunction(loader, clickhouse_attach_function);
}

} // namespace duckdb

