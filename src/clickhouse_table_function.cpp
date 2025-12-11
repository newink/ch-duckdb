#include "clickhouse_table_function.hpp"
#include "clickhouse_scan_table_function.hpp"

#include "clickhouse_catalog.hpp"
#include "clickhouse_connection.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/uuid.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/logging/logger.hpp"

#include <clickhouse/client.h>
#include <clickhouse/columns/array.h>
#include <clickhouse/columns/date.h>
#include <clickhouse/columns/lowcardinality.h>
#include <clickhouse/columns/numeric.h>
#include <clickhouse/columns/string.h>
#include <clickhouse/columns/uuid.h>
#include <clickhouse/columns/enum.h>

#include <cmath>
#include <sstream>
#include <cstdarg>
#include <cstdio>
#include <cstring>

namespace duckdb {

namespace {

static bool
DebugEnabled() {
	static bool cached = false;
	static bool initialized = false;
	if (!initialized) {
		const char *env = std::getenv("CH_DUCKDB_DEBUG");
		cached = env && std::strlen(env) > 0;
		initialized = true;
	}
	return cached;
}

static void
DebugPrint(const char *fmt, ...) {
	if (!DebugEnabled()) {
		return;
	}
	va_list args;
	va_start(args, fmt);
	std::vfprintf(stderr, fmt, args);
	std::fprintf(stderr, "\n");
	va_end(args);
}

static string
HexPreview(std::string_view sv, idx_t max_bytes = 32) {
	static const char hex_digits[] = "0123456789ABCDEF";
	string out;
	out.reserve(sv.size() * 2);
	for (idx_t i = 0; i < sv.size() && i < max_bytes; i++) {
		auto c = static_cast<uint8_t>(sv[i]);
		out.push_back(hex_digits[c >> 4]);
		out.push_back(hex_digits[c & 0x0F]);
	}
	if (sv.size() > max_bytes) {
		out.append("...");
	}
	return out;
}

struct ClickhouseFunctionBindData : public TableFunctionData {
	string function_name;
	string query;
	ClickhouseConnectionConfig config;
	vector<string> column_names;
	vector<clickhouse::TypeRef> column_types;
};

struct ClickhouseQueryGlobalState : public GlobalTableFunctionState {
	explicit ClickhouseQueryGlobalState(vector<clickhouse::Block> blocks_p)
	    : blocks(std::move(blocks_p)), block_idx(0), row_idx(0) {
	}

	idx_t MaxThreads() const override {
		return 1;
	}

	vector<clickhouse::Block> blocks;
	idx_t block_idx;
	idx_t row_idx;
};

static string
Redacted(const ClickhouseConnectionConfig &config) {
	std::ostringstream oss;
	oss << "host=" << config.host << " port=" << config.port << " db=" << config.database << " user=" << config.user;
	if (!config.password.empty()) {
		oss << " password=<redacted>";
	}
	if (config.secure) {
		oss << " secure=true";
	}
	return oss.str();
}

static clickhouse::ClientOptions
BuildClientOptions(const ClickhouseConnectionConfig &config) {
	clickhouse::ClientOptions opts;
	opts.SetHost(config.host)
	    .SetPort(config.port)
	    .SetDefaultDatabase(config.database)
	    .SetUser(config.user)
	    .SetPassword(config.password);

	if (config.secure) {
		clickhouse::ClientOptions::SSLOptions ssl_opts;
		ssl_opts.SetUseDefaultCALocations(true);
		opts.SetSSLOptions(ssl_opts);
	}
	DebugPrint("[ch_duckdb] client opts host=%s port=%d db=%s secure=%d", config.host.c_str(), config.port,
	           config.database.c_str(), config.secure ? 1 : 0);
	return opts;
}

static inline size_t
GetActualStringLength(std::string_view sv) {
	// Find the first null byte
	size_t null_pos = sv.find('\0');
	size_t max_len = (null_pos == std::string_view::npos) ? sv.size() : null_pos;

	// Validate UTF-8 and truncate at first invalid byte
	size_t valid_len = 0;
	for (size_t i = 0; i < max_len; ) {
		unsigned char c = sv[i];
		size_t char_len;

		// Determine UTF-8 character length
		if (c < 0x80) {
			char_len = 1;
		} else if ((c & 0xE0) == 0xC0) {
			char_len = 2;
		} else if ((c & 0xF0) == 0xE0) {
			char_len = 3;
		} else if ((c & 0xF8) == 0xF0) {
			char_len = 4;
		} else {
			// Invalid UTF-8 start byte, stop here
			break;
		}

		// Check if we have enough bytes
		if (i + char_len > max_len) {
			break;
		}

		// Validate continuation bytes
		bool valid = true;
		for (size_t j = 1; j < char_len; j++) {
			if ((sv[i + j] & 0xC0) != 0x80) {
				valid = false;
				break;
			}
		}

		if (!valid) {
			break;
		}

		valid_len = i + char_len;
		i += char_len;
	}

	return valid_len;
}

static LogicalType
MapClickhouseType(const clickhouse::TypeRef &type) {
	switch (type->GetCode()) {
	case clickhouse::Type::Nullable:
		return MapClickhouseType(type->As<clickhouse::NullableType>()->GetNestedType());
	case clickhouse::Type::LowCardinality:
		return MapClickhouseType(type->As<clickhouse::LowCardinalityType>()->GetNestedType());
	case clickhouse::Type::Array:
		return LogicalType::LIST(MapClickhouseType(type->As<clickhouse::ArrayType>()->GetItemType()));
	case clickhouse::Type::Int8:
		return LogicalType::TINYINT;
	case clickhouse::Type::Int16:
		return LogicalType::SMALLINT;
	case clickhouse::Type::Int32:
		return LogicalType::INTEGER;
	case clickhouse::Type::Int64:
		return LogicalType::BIGINT;
	case clickhouse::Type::UInt8:
		return LogicalType::UTINYINT;
	case clickhouse::Type::UInt16:
		return LogicalType::USMALLINT;
	case clickhouse::Type::UInt32:
		return LogicalType::UINTEGER;
	case clickhouse::Type::UInt64:
		return LogicalType::UBIGINT;
	case clickhouse::Type::Float32:
		return LogicalType::FLOAT;
	case clickhouse::Type::Float64:
		return LogicalType::DOUBLE;
	case clickhouse::Type::String:
	case clickhouse::Type::FixedString:
		return LogicalType::VARCHAR;
	case clickhouse::Type::Enum8:
	case clickhouse::Type::Enum16:
		return LogicalType::VARCHAR;
	case clickhouse::Type::UUID:
		return LogicalType::UUID;
	case clickhouse::Type::Date:
	case clickhouse::Type::Date32:
		return LogicalType::DATE;
	case clickhouse::Type::DateTime:
	case clickhouse::Type::DateTime64:
		return LogicalType::TIMESTAMP;
	case clickhouse::Type::Tuple: {
		auto items = type->As<clickhouse::TupleType>()->GetTupleType();
		child_list_t<LogicalType> children;
		children.reserve(items.size());
		for (idx_t i = 0; i < items.size(); i++) {
			children.emplace_back(StringUtil::Format("_%d", i + 1), MapClickhouseType(items[i]));
		}
		return LogicalType::STRUCT(children);
	}
	default:
		throw BinderException("ClickHouse table function: unsupported ClickHouse type '%s'", type->GetName());
	}
}

static vector<clickhouse::Block>
ExecuteQuery(const ClickhouseFunctionBindData &bind_data, ClientContext &context) {
	auto opts = BuildClientOptions(bind_data.config);
	DebugPrint("[%s] connecting: %s", bind_data.function_name.c_str(), Redacted(bind_data.config).c_str());
	clickhouse::Client client(opts);
	vector<clickhouse::Block> blocks;
	DebugPrint("[%s] executing: %s", bind_data.function_name.c_str(), bind_data.query.c_str());
	client.Select(bind_data.query, [&](const clickhouse::Block &block) { blocks.push_back(block); });
	if (DebugEnabled()) {
		DebugPrint("[%s] received %zu blocks", bind_data.function_name.c_str(), blocks.size());
		for (idx_t bi = 0; bi < blocks.size(); bi++) {
			auto &b = blocks[bi];
			DebugPrint("  block %llu rows=%zu cols=%zu", static_cast<unsigned long long>(bi), b.GetRowCount(),
			           b.GetColumnCount());
			for (size_t i = 0; i < b.GetColumnCount(); i++) {
				DebugPrint("    col %zu name=%s type=%s", i, b.GetColumnName(i).c_str(),
				           b[i]->Type()->GetName().c_str());
				if (b.GetRowCount() > 0 && b[i]->Type()->GetCode() == clickhouse::Type::String) {
					auto sv = b[i]->AsStrict<clickhouse::ColumnString>()->At(0);
					DebugPrint("      first row hex=%s len=%zu", HexPreview(sv).c_str(), sv.size());
				}
			}
		}
		if (blocks.empty()) {
			DebugPrint("[%s] received no blocks", bind_data.function_name.c_str());
		}
	}
	return blocks;
}

static void
DiscoverSchema(ClickhouseFunctionBindData &bind_data, vector<LogicalType> &return_types, vector<string> &names, ClientContext &context) {
	auto opts = BuildClientOptions(bind_data.config);
	DebugPrint("[%s] schema probe connect: %s", bind_data.function_name.c_str(), Redacted(bind_data.config).c_str());
	clickhouse::Client client(opts);
	auto probe_query = "SELECT * FROM (" + bind_data.query + ") LIMIT 0";
	vector<clickhouse::Block> probe_blocks;
	client.Select(probe_query, [&](const clickhouse::Block &block) { probe_blocks.push_back(block); });
	if (probe_blocks.empty() || probe_blocks.front().GetColumnCount() == 0) {
		throw BinderException("%s: unable to discover schema (no columns returned)", bind_data.function_name.c_str());
	}

	auto &block = probe_blocks.front();
	for (size_t col_idx = 0; col_idx < block.GetColumnCount(); col_idx++) {
		auto type = block[col_idx]->Type();
		return_types.push_back(MapClickhouseType(type));
		auto name = block.GetColumnName(col_idx);
		names.push_back(name);
		bind_data.column_names.push_back(name);
		bind_data.column_types.push_back(type);
		DebugPrint("[%s] column %zu name=%s type=%s", bind_data.function_name.c_str(), col_idx, name.c_str(),
		           type->GetName().c_str());
	}
}

static ClickhouseConnectionConfig
BuildConfigFromInputs(TableFunctionBindInput &input) {
	ClickhouseConnectionConfig config;
	config.host = input.inputs[1].ToString();

	if (input.inputs.size() > 2 && !input.inputs[2].IsNull()) {
		auto port_val = IntegerValue::Get(input.inputs[2]);
		if (port_val > NumericLimits<uint16_t>::Maximum()) {
			throw BinderException("clickhouse_scan: port '%d' exceeds uint16_t range", port_val);
		}
		config.port = static_cast<uint16_t>(port_val);
	}
	if (input.inputs.size() > 3 && !input.inputs[3].IsNull()) {
		config.user = input.inputs[3].ToString();
	}
	if (input.inputs.size() > 4 && !input.inputs[4].IsNull()) {
		config.password = input.inputs[4].ToString();
	}
	if (input.inputs.size() > 5 && !input.inputs[5].IsNull()) {
		config.database = input.inputs[5].ToString();
	}
	if (input.inputs.size() > 6 && !input.inputs[6].IsNull()) {
		config.secure = BooleanValue::Get(input.inputs[6]);
	}
	return config;
}

static ClickhouseConnectionConfig
LookupConfigFromAlias(ClientContext &context, const string &alias) {
	auto attached = DatabaseManager::Get(context).GetDatabase(context, alias);
	if (!attached) {
		throw BinderException("clickhouse_query: unknown attached database alias \"%s\"", alias.c_str());
	}
	auto &catalog = attached->GetCatalog();
	if (catalog.GetCatalogType() != "clickhouse") {
		throw BinderException("clickhouse_query: database \"%s\" is not a ClickHouse catalog", alias.c_str());
	}
	auto *ch_catalog = dynamic_cast<ClickhouseCatalog *>(&catalog);
	if (!ch_catalog) {
		throw BinderException("clickhouse_query: internal error resolving catalog for \"%s\"", alias.c_str());
	}
	return ch_catalog->GetConnectionConfig();
}

static unique_ptr<FunctionData>
ClickhouseQueryBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                    vector<string> &names) {
	if (input.inputs.size() < 2) {
		throw BinderException("clickhouse_scan requires at least query and host arguments");
	}

	auto bind_data = make_uniq<ClickhouseFunctionBindData>();
	bind_data->function_name = "clickhouse_scan";
	bind_data->query = input.inputs[0].ToString();
	bind_data->config = BuildConfigFromInputs(input);

	DiscoverSchema(*bind_data, return_types, names, context);
	return std::move(bind_data);
}

static unique_ptr<FunctionData>
ClickhouseScanBind(ClientContext &context, TableFunctionBindInput &input, vector<LogicalType> &return_types,
                   vector<string> &names) {
	if (input.inputs.size() < 2) {
		throw BinderException("clickhouse_query requires alias and query arguments");
	}

	auto bind_data = make_uniq<ClickhouseFunctionBindData>();
	bind_data->function_name = "clickhouse_query";
	auto alias = input.inputs[0].ToString();
	bind_data->query = input.inputs[1].ToString();
	bind_data->config = LookupConfigFromAlias(context, alias);

	DiscoverSchema(*bind_data, return_types, names, context);
	return std::move(bind_data);
}

static unique_ptr<GlobalTableFunctionState>
ClickhouseQueryInitGlobal(ClientContext &context, TableFunctionInitInput &input) {
	auto &bind_data = input.bind_data->Cast<ClickhouseFunctionBindData>();
	auto blocks = ExecuteQuery(bind_data, context);
	return make_uniq<ClickhouseQueryGlobalState>(std::move(blocks));
}

static int64_t
DateTime64ToMicros(int64_t raw, size_t precision) {
	int64_t scale = 1;
	for (size_t i = 0; i < precision; i++) {
		scale *= 10;
	}
	if (scale == 0) {
		return 0;
	}
	return (raw * 1000000) / scale;
}

static void CopyFromItemView(const clickhouse::ItemView &item, const clickhouse::TypeRef &type, Vector &target,
                             idx_t target_idx) {
	if (item.type == clickhouse::Type::Void) {
		FlatVector::SetNull(target, target_idx, true);
		return;
	}

	switch (type->GetCode()) {
	case clickhouse::Type::Int8:
		FlatVector::GetData<int8_t>(target)[target_idx] = item.get<int8_t>();
		break;
	case clickhouse::Type::Int16:
		FlatVector::GetData<int16_t>(target)[target_idx] = item.get<int16_t>();
		break;
	case clickhouse::Type::Int32:
		FlatVector::GetData<int32_t>(target)[target_idx] = item.get<int32_t>();
		break;
	case clickhouse::Type::Int64:
		FlatVector::GetData<int64_t>(target)[target_idx] = item.get<int64_t>();
		break;
	case clickhouse::Type::UInt8:
		FlatVector::GetData<uint8_t>(target)[target_idx] = item.get<uint8_t>();
		break;
	case clickhouse::Type::UInt16:
		FlatVector::GetData<uint16_t>(target)[target_idx] = item.get<uint16_t>();
		break;
	case clickhouse::Type::UInt32:
		FlatVector::GetData<uint32_t>(target)[target_idx] = item.get<uint32_t>();
		break;
	case clickhouse::Type::UInt64:
		FlatVector::GetData<uint64_t>(target)[target_idx] = item.get<uint64_t>();
		break;
	case clickhouse::Type::Float32:
		FlatVector::GetData<float>(target)[target_idx] = item.get<float>();
		break;
	case clickhouse::Type::Float64:
		FlatVector::GetData<double>(target)[target_idx] = item.get<double>();
		break;
	case clickhouse::Type::String: {
		auto sv = item.get<std::string_view>();
		FlatVector::GetData<string_t>(target)[target_idx] = StringVector::AddString(target, sv.data(), sv.size());
		break;
	}
	case clickhouse::Type::FixedString: {
		auto sv = item.get<std::string_view>();
		size_t actual_len = GetActualStringLength(sv);
		FlatVector::GetData<string_t>(target)[target_idx] = StringVector::AddString(target, sv.data(), actual_len);
		break;
	}
	case clickhouse::Type::Date: {
		auto days = item.get<uint16_t>();
		FlatVector::GetData<date_t>(target)[target_idx] = date_t(int32_t(days));
		break;
	}
	case clickhouse::Type::Date32: {
		auto days = item.get<int32_t>();
		FlatVector::GetData<date_t>(target)[target_idx] = date_t(int32_t(days));
		break;
	}
	case clickhouse::Type::DateTime: {
		auto seconds = item.get<uint32_t>();
		FlatVector::GetData<timestamp_t>(target)[target_idx] = Timestamp::FromEpochSeconds(seconds);
		break;
	}
	case clickhouse::Type::DateTime64: {
		auto raw = static_cast<int64_t>(item.get<clickhouse::Int128>());
		auto precision = type->As<clickhouse::DateTime64Type>()->GetPrecision();
		auto micros = DateTime64ToMicros(raw, precision);
		FlatVector::GetData<timestamp_t>(target)[target_idx] = Timestamp::FromEpochMicroSeconds(micros);
		break;
	}
	case clickhouse::Type::Enum8: {
		auto enum_type = type->As<clickhouse::EnumType>();
		auto value = item.get<int8_t>();
		auto name = enum_type->GetEnumName(value);
		FlatVector::GetData<string_t>(target)[target_idx] = StringVector::AddString(target, name.data(), name.size());
		break;
	}
	case clickhouse::Type::Enum16: {
		auto enum_type = type->As<clickhouse::EnumType>();
		auto value = item.get<int16_t>();
		auto name = enum_type->GetEnumName(value);
		FlatVector::GetData<string_t>(target)[target_idx] = StringVector::AddString(target, name.data(), name.size());
		break;
	}
	case clickhouse::Type::Tuple: {
		auto tuple_type = type->As<clickhouse::TupleType>();
		auto &entries = StructVector::GetEntries(target);
		for (idx_t i = 0; i < entries.size(); i++) {
			FlatVector::SetNull(target, target_idx, true);
			(void)tuple_type;
		}
		break;
	}
	default:
		throw InvalidInputException("ClickHouse function: unsupported low-cardinality type in execution (%s)",
		                            type->GetName());
	}
}

static void CopyValue(const clickhouse::ColumnRef &col, const clickhouse::TypeRef &type, idx_t row, Vector &target,
                      idx_t out_idx);

static void CopyArray(const clickhouse::ColumnRef &col, const clickhouse::TypeRef &type, idx_t row, Vector &target,
                      idx_t out_idx) {
	auto array_type = type->As<clickhouse::ArrayType>();
	auto array_col = col->AsStrict<clickhouse::ColumnArray>();
	auto nested_col = array_col->GetAsColumn(row);
	auto length = nested_col->Size();

	auto current_size = ListVector::GetListSize(target);
	ListVector::Reserve(target, current_size + length);
	auto list_entries = ListVector::GetData(target);
	list_entries[out_idx].offset = current_size;
	list_entries[out_idx].length = length;

	auto &child = ListVector::GetEntry(target);
	for (idx_t i = 0; i < length; i++) {
		CopyValue(nested_col, array_type->GetItemType(), i, child, current_size + i);
	}
	ListVector::SetListSize(target, current_size + length);
}

static void CopyValue(const clickhouse::ColumnRef &col, const clickhouse::TypeRef &type, idx_t row, Vector &target,
                      idx_t out_idx) {
	switch (type->GetCode()) {
	case clickhouse::Type::Nullable: {
		auto nullable_col = col->AsStrict<clickhouse::ColumnNullable>();
		if (nullable_col->IsNull(row)) {
			FlatVector::SetNull(target, out_idx, true);
			return;
		}
		auto nested_type = type->As<clickhouse::NullableType>()->GetNestedType();
		CopyValue(nullable_col->Nested(), nested_type, row, target, out_idx);
		return;
	}
	case clickhouse::Type::LowCardinality: {
		auto lc_col = col->AsStrict<clickhouse::ColumnLowCardinality>();
		auto nested_type = type->As<clickhouse::LowCardinalityType>()->GetNestedType();
		auto item = lc_col->GetItem(row);
		CopyFromItemView(item, nested_type, target, out_idx);
		return;
	}
	case clickhouse::Type::Array: {
		CopyArray(col, type, row, target, out_idx);
		return;
	}
	case clickhouse::Type::Tuple: {
		auto tuple_col = col->AsStrict<clickhouse::ColumnTuple>();
		auto tuple_type = type->As<clickhouse::TupleType>();
		auto &entries = StructVector::GetEntries(target);
		for (idx_t i = 0; i < tuple_col->TupleSize(); i++) {
			CopyValue(tuple_col->At(i), tuple_type->GetTupleType()[i], row, *entries[i], out_idx);
		}
		return;
	}
	case clickhouse::Type::Int8:
		FlatVector::GetData<int8_t>(target)[out_idx] = col->AsStrict<clickhouse::ColumnInt8>()->At(row);
		return;
	case clickhouse::Type::Int16:
		FlatVector::GetData<int16_t>(target)[out_idx] = col->AsStrict<clickhouse::ColumnInt16>()->At(row);
		return;
	case clickhouse::Type::Int32:
		FlatVector::GetData<int32_t>(target)[out_idx] = col->AsStrict<clickhouse::ColumnInt32>()->At(row);
		return;
	case clickhouse::Type::Int64:
		FlatVector::GetData<int64_t>(target)[out_idx] = col->AsStrict<clickhouse::ColumnInt64>()->At(row);
		return;
	case clickhouse::Type::UInt8:
		FlatVector::GetData<uint8_t>(target)[out_idx] = col->AsStrict<clickhouse::ColumnUInt8>()->At(row);
		return;
	case clickhouse::Type::UInt16:
		FlatVector::GetData<uint16_t>(target)[out_idx] = col->AsStrict<clickhouse::ColumnUInt16>()->At(row);
		return;
	case clickhouse::Type::UInt32:
		FlatVector::GetData<uint32_t>(target)[out_idx] = col->AsStrict<clickhouse::ColumnUInt32>()->At(row);
		return;
	case clickhouse::Type::UInt64:
		FlatVector::GetData<uint64_t>(target)[out_idx] = col->AsStrict<clickhouse::ColumnUInt64>()->At(row);
		return;
	case clickhouse::Type::Float32:
		FlatVector::GetData<float>(target)[out_idx] = col->AsStrict<clickhouse::ColumnFloat32>()->At(row);
		return;
	case clickhouse::Type::Float64:
		FlatVector::GetData<double>(target)[out_idx] = col->AsStrict<clickhouse::ColumnFloat64>()->At(row);
		return;
	case clickhouse::Type::UUID: {
		auto uuid_col = col->AsStrict<clickhouse::ColumnUUID>();
		auto uuid_val = uuid_col->At(row);
		uint64_t low = uuid_val.first;
		uint64_t high = uuid_val.second;
		uhugeint_t combined;
		combined.lower = low;
		combined.upper = high;
		auto huge = UUID::FromUHugeint(combined);
		FlatVector::GetData<hugeint_t>(target)[out_idx] = huge;
		return;
	}
	case clickhouse::Type::String: {
		auto sv = col->AsStrict<clickhouse::ColumnString>()->At(row);
		if (DebugEnabled() && row < 5) {
			DebugPrint("[String] row=%llu len=%zu hex=%s", static_cast<long long>(row), sv.size(),
			           HexPreview(sv).c_str());
		}
		FlatVector::GetData<string_t>(target)[out_idx] = StringVector::AddString(target, sv.data(), sv.size());
		return;
	}
	case clickhouse::Type::FixedString: {
		auto sv = col->AsStrict<clickhouse::ColumnFixedString>()->At(row);
		if (DebugEnabled() && row < 5) {
			DebugPrint("[FixedString] row=%llu len=%zu hex=%s", static_cast<long long>(row), sv.size(),
			           HexPreview(sv).c_str());
		}
		size_t actual_len = GetActualStringLength(sv);
		FlatVector::GetData<string_t>(target)[out_idx] = StringVector::AddString(target, sv.data(), actual_len);
		return;
	}
	case clickhouse::Type::Date: {
		auto days = col->AsStrict<clickhouse::ColumnDate>()->At(row);
		FlatVector::GetData<date_t>(target)[out_idx] = date_t(int32_t(days));
		return;
	}
	case clickhouse::Type::Date32: {
		auto days = col->AsStrict<clickhouse::ColumnDate32>()->At(row);
		FlatVector::GetData<date_t>(target)[out_idx] = date_t(int32_t(days));
		return;
	}
	case clickhouse::Type::DateTime: {
		auto seconds = col->AsStrict<clickhouse::ColumnDateTime>()->RawAt(row);
		FlatVector::GetData<timestamp_t>(target)[out_idx] = Timestamp::FromEpochSeconds(seconds);
		return;
	}
	case clickhouse::Type::DateTime64: {
		auto dt64_col = col->AsStrict<clickhouse::ColumnDateTime64>();
		auto micros = DateTime64ToMicros(dt64_col->At(row), dt64_col->GetPrecision());
		FlatVector::GetData<timestamp_t>(target)[out_idx] = Timestamp::FromEpochMicroSeconds(micros);
		return;
	}
	case clickhouse::Type::Enum8: {
		auto enum_col = col->AsStrict<clickhouse::ColumnEnum8>();
		auto name = enum_col->NameAt(row);
		FlatVector::GetData<string_t>(target)[out_idx] = StringVector::AddString(target, name.data(), name.size());
		return;
	}
	case clickhouse::Type::Enum16: {
		auto enum_col = col->AsStrict<clickhouse::ColumnEnum16>();
		auto name = enum_col->NameAt(row);
		FlatVector::GetData<string_t>(target)[out_idx] = StringVector::AddString(target, name.data(), name.size());
		return;
	}
	default:
		throw InvalidInputException("ClickHouse function: unsupported type in execution (%s)", type->GetName());
	}
}

static void
ClickhouseQueryExecute(ClientContext &, TableFunctionInput &input, DataChunk &output) {
	auto &global_state = input.global_state->Cast<ClickhouseQueryGlobalState>();
	auto &bind_data = input.bind_data->Cast<ClickhouseFunctionBindData>();
	idx_t out_offset = 0;

	while (out_offset < STANDARD_VECTOR_SIZE && global_state.block_idx < global_state.blocks.size()) {
		auto &block = global_state.blocks[global_state.block_idx];
		auto rows_in_block = static_cast<idx_t>(block.GetRowCount());
		if (DebugEnabled()) {
			DebugPrint("[execute] block_idx=%llu rows=%llu cols=%llu row_idx=%llu out_offset=%llu",
			           static_cast<unsigned long long>(global_state.block_idx),
			           static_cast<unsigned long long>(rows_in_block),
			           static_cast<unsigned long long>(block.GetColumnCount()),
			           static_cast<unsigned long long>(global_state.row_idx),
			           static_cast<unsigned long long>(out_offset));
		}
		if (rows_in_block == 0) {
			global_state.block_idx++;
			global_state.row_idx = 0;
			continue;
		}

		auto copy_count = MinValue<idx_t>(STANDARD_VECTOR_SIZE - out_offset, rows_in_block - global_state.row_idx);
		for (size_t col_idx = 0; col_idx < block.GetColumnCount(); col_idx++) {
			auto col = block[col_idx];
			auto &target = output.data[col_idx];
			for (idx_t i = 0; i < copy_count; i++) {
				auto row = global_state.row_idx + i;
				CopyValue(col, bind_data.column_types[col_idx], row, target, out_offset + i);
			}
		}

		out_offset += copy_count;
		global_state.row_idx += copy_count;
		if (global_state.row_idx >= rows_in_block) {
			global_state.block_idx++;
			global_state.row_idx = 0;
		}
	}

	output.SetCardinality(out_offset);
	if (DebugEnabled() && out_offset > 0 && output.ColumnCount() > 0) {
		auto &v = output.data[0];
		auto val = FlatVector::GetData<string_t>(v)[0];
		auto sv = val.GetString();
		DebugPrint("[execute] first row after copy len=%zu hex=%s", sv.size(), HexPreview(sv).c_str());
	}
}

} // namespace

TableFunction
ClickhouseQueryFunction::GetFunction() {
	// clickhouse_scan(query, host, port, user, password, database, secure)
	TableFunction fun("clickhouse_scan",
	                  {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::INTEGER, LogicalType::VARCHAR,
	                   LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BOOLEAN},
	                  ClickhouseQueryExecute, ClickhouseQueryBind, ClickhouseQueryInitGlobal);
	fun.projection_pushdown = false;
	return fun;
}

TableFunction
ClickhouseScanFunction::GetFunction() {
	// clickhouse_query(alias, query)
	TableFunction fun("clickhouse_query", {LogicalType::VARCHAR, LogicalType::VARCHAR}, ClickhouseQueryExecute,
	                  ClickhouseScanBind, ClickhouseQueryInitGlobal);
	fun.projection_pushdown = false;
	return fun;
}

} // namespace duckdb
