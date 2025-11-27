#pragma once

#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct ClickhouseScanFunction {
	static TableFunction GetFunction();
};

} // namespace duckdb
