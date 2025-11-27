#include "clickhouse_catalog.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/to_string.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/parser/parsed_data/attach_info.hpp"
#include "clickhouse_connection.hpp"
#include "duckdb/transaction/transaction.hpp"

#include <clickhouse/client.h>
#include <clickhouse/exceptions.h>
#include <iostream>
#include <sstream>

namespace duckdb {

ClickhouseCatalog::ClickhouseCatalog(AttachedDatabase &db, const string &connection_string_p,
                                     unordered_map<string, Value> options_map, ClickhouseConnectionConfig config_p)
    : Catalog(db), connection_string(connection_string_p), options(std::move(options_map)), config(std::move(config_p)) {
}

namespace {

static const Value *
FindOption(const unordered_map<string, Value> &opts, const string &key) {
	auto lower_key = StringUtil::Lower(key);
	for (auto &entry : opts) {
		if (StringUtil::Lower(entry.first) == lower_key) {
			return &entry.second;
		}
	}
	return nullptr;
}

static void ApplyStringOption(const unordered_map<string, Value> &opts, const string &key, string &target) {
	if (auto val = FindOption(opts, key)) {
		target = val->ToString();
	}
}

static void
ApplyBoolOption(const unordered_map<string, Value> &opts, const string &key, bool &target) {
	if (auto val = FindOption(opts, key)) {
		target = BooleanValue::Get(*val);
	}
}

static void ApplyPortOption(const unordered_map<string, Value> &opts, const string &key, uint16_t &target) {
	if (auto val = FindOption(opts, key)) {
		auto port_str = val->ToString();
		auto port_value = std::stoul(port_str);
		if (port_value > NumericLimits<uint16_t>::Maximum()) {
			throw InvalidInputException("clickhouse attach: port '%s' exceeds uint16_t range", port_str.c_str());
		}
		target = static_cast<uint16_t>(port_value);
	}
}

static void
ParsePath(const string &path, ClickhouseConnectionConfig &config) {
	if (path.empty()) {
		return;
	}

	string trimmed = path;
	auto scheme_pos = trimmed.find("://");
	if (scheme_pos != string::npos) {
		trimmed = trimmed.substr(scheme_pos + 3);
	}

	auto slash_pos = trimmed.find('/');
	if (slash_pos != string::npos) {
		config.database = trimmed.substr(slash_pos + 1);
		trimmed = trimmed.substr(0, slash_pos);
	}

	auto colon_pos = trimmed.rfind(':');
	if (colon_pos != string::npos && colon_pos + 1 < trimmed.size()) {
		auto port_str = trimmed.substr(colon_pos + 1);
		try {
			auto port_num = std::stoul(port_str);
			if (port_num > NumericLimits<uint16_t>::Maximum()) {
				throw InvalidInputException("clickhouse attach: port '%s' exceeds uint16_t range", port_str.c_str());
			}
			config.port = static_cast<uint16_t>(port_num);
		} catch (std::exception &ex) {
			throw InvalidInputException("clickhouse attach: unable to parse port '%s': %s", port_str.c_str(),
			                            ex.what());
		}
		config.host = trimmed.substr(0, colon_pos);
	} else {
		config.host = trimmed;
	}
}

static ClickhouseConnectionConfig
BuildConfig(const string &path, const unordered_map<string, Value> &opts) {
	ClickhouseConnectionConfig config;
	ParsePath(path, config);

	ApplyStringOption(opts, "host", config.host);
	ApplyPortOption(opts, "port", config.port);
	ApplyStringOption(opts, "database", config.database);
	ApplyStringOption(opts, "db", config.database);
	ApplyStringOption(opts, "user", config.user);
	ApplyStringOption(opts, "username", config.user);
	ApplyStringOption(opts, "password", config.password);
	ApplyBoolOption(opts, "secure", config.secure);

	if (config.host.empty()) {
		throw InvalidInputException("clickhouse attach: host must be provided via ATTACH path or OPTIONS");
	}
	return config;
}

static std::string
Redacted(const ClickhouseConnectionConfig &cfg) {
	std::ostringstream oss;
	oss << "host=" << cfg.host << " port=" << cfg.port << " db=" << cfg.database << " user=" << cfg.user;
	if (!cfg.password.empty()) {
		oss << " password=<redacted>";
	}
	if (cfg.secure) {
		oss << " secure=true";
	}
	return oss.str();
}

static void
VerifyConnection(const ClickhouseConnectionConfig &cfg) {
	clickhouse::ClientOptions client_opts;
	client_opts.SetHost(cfg.host)
	    .SetPort(cfg.port)
	    .SetDefaultDatabase(cfg.database)
	    .SetUser(cfg.user)
	    .SetPassword(cfg.password);

	if (cfg.secure) {
		// Minimum SSL setup: rely on defaults when secure=true.
		clickhouse::ClientOptions::SSLOptions ssl_opts;
		ssl_opts.SetUseDefaultCALocations(true);
		client_opts.SetSSLOptions(ssl_opts);
	}

	std::cerr << "[clickhouse] attempting connection: " << Redacted(cfg) << std::endl;
	clickhouse::Client client(client_opts);
	client.Ping();
	std::cerr << "[clickhouse] connection verified" << std::endl;
}

} // namespace

unique_ptr<Catalog>
ClickhouseCatalog::Attach(optional_ptr<StorageExtensionInfo>, ClientContext &, AttachedDatabase &db,
                          const string &, AttachInfo &info, AttachOptions &) {
	auto config = BuildConfig(info.path, info.options);
	try {
		VerifyConnection(config);
	} catch (const clickhouse::ServerException &ex) {
		std::cerr << "[clickhouse] server error: " << ex.what() << std::endl;
		throw;
	} catch (const std::exception &ex) {
		std::cerr << "[clickhouse] connection failed: " << ex.what() << std::endl;
		throw;
	}
	return make_uniq<ClickhouseCatalog>(db, info.path, std::move(info.options), std::move(config));
}

void
ClickhouseCatalog::Initialize(bool) {
	// Defer remote discovery until objects are referenced.
}

string
ClickhouseCatalog::GetCatalogType() {
	return "clickhouse";
}

optional_ptr<CatalogEntry>
ClickhouseCatalog::CreateSchema(CatalogTransaction, CreateSchemaInfo &) {
	throw NotImplementedException("CreateSchema is not supported for ClickHouse catalogs");
}

optional_ptr<SchemaCatalogEntry>
ClickhouseCatalog::LookupSchema(CatalogTransaction, const EntryLookupInfo &schema_lookup,
                                OnEntryNotFound if_not_found) {
	if (if_not_found == OnEntryNotFound::THROW_EXCEPTION) {
		throw CatalogException("Schema \"%s\" does not exist in ClickHouse catalog \"%s\"",
		                       schema_lookup.GetEntryName(), GetName());
	}
	return nullptr;
}

void
ClickhouseCatalog::ScanSchemas(ClientContext &, std::function<void(SchemaCatalogEntry &)>) {
	// Schema enumeration will be wired once metadata discovery is implemented.
}

PhysicalOperator &
ClickhouseCatalog::PlanCreateTableAs(ClientContext &, PhysicalPlanGenerator &, LogicalCreateTable &,
                                     PhysicalOperator &) {
	throw NotImplementedException("CREATE TABLE AS is not supported for ClickHouse catalogs");
}

PhysicalOperator &
ClickhouseCatalog::PlanInsert(ClientContext &, PhysicalPlanGenerator &, LogicalInsert &,
                              optional_ptr<PhysicalOperator>) {
	throw NotImplementedException("INSERT is not supported for ClickHouse catalogs");
}

PhysicalOperator &
ClickhouseCatalog::PlanDelete(ClientContext &, PhysicalPlanGenerator &, LogicalDelete &, PhysicalOperator &) {
	throw NotImplementedException("DELETE is not supported for ClickHouse catalogs");
}

PhysicalOperator &
ClickhouseCatalog::PlanUpdate(ClientContext &, PhysicalPlanGenerator &, LogicalUpdate &, PhysicalOperator &) {
	throw NotImplementedException("UPDATE is not supported for ClickHouse catalogs");
}

unique_ptr<LogicalOperator>
ClickhouseCatalog::BindCreateIndex(Binder &, CreateStatement &, TableCatalogEntry &,
                                   unique_ptr<LogicalOperator>) {
	throw NotImplementedException("CREATE INDEX is not supported for ClickHouse catalogs");
}

DatabaseSize
ClickhouseCatalog::GetDatabaseSize(ClientContext &) {
	return DatabaseSize();
}

bool
ClickhouseCatalog::InMemory() {
	return false;
}

string
ClickhouseCatalog::GetDBPath() {
	return connection_string;
}

void
ClickhouseCatalog::DropSchema(ClientContext &, DropInfo &) {
	throw NotImplementedException("DROP SCHEMA is not supported for ClickHouse catalogs");
}

const ClickhouseConnectionConfig &
ClickhouseCatalog::GetConnectionConfig() const {
	return config;
}

} // namespace duckdb
