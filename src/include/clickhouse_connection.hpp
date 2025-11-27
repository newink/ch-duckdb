#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct ClickhouseConnectionConfig {
	string host;
	uint16_t port = 9000;
	string database = "default";
	string user = "default";
	string password;
	bool secure = false;
};

} // namespace duckdb
