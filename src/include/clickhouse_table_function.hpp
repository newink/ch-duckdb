#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ClickhouseQueryFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb
