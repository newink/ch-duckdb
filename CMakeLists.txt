cmake_minimum_required(VERSION 3.5)

# Set extension name here
set(TARGET_NAME ch_duckdb)

# Ensure we can build ClickHouse client (requires C++17 features)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})
include_directories(src/include)

set(EXTENSION_SOURCES
    src/ch_duckdb_extension.cpp
    src/clickhouse_catalog.cpp
    src/clickhouse_storage_extension.cpp
    src/clickhouse_transaction.cpp
    src/clickhouse_transaction_manager.cpp
    src/clickhouse_table_function.cpp
)

# Integrate clickhouse-cpp from local submodule
set(CLICKHOUSE_CPP_DIR ${CMAKE_CURRENT_SOURCE_DIR}/third_party/clickhouse-cpp)
set(BUILD_TESTS OFF CACHE BOOL "Disable clickhouse-cpp tests" FORCE)
set(BUILD_EXAMPLES OFF CACHE BOOL "Disable clickhouse-cpp examples" FORCE)
add_subdirectory(${CLICKHOUSE_CPP_DIR} EXCLUDE_FROM_ALL)

# DuckDB namespaces its bundled zstd symbols as duckdb_zstd; include an alias header so clickhouse-cpp can resolve unqualified ZSTD_* calls.
# Applied unconditionally since the header has internal guards.
target_compile_options(clickhouse-cpp-lib PRIVATE -include ${CMAKE_CURRENT_SOURCE_DIR}/src/include/ch_duckdb_zstd_alias.hpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
target_include_directories(${EXTENSION_NAME} PRIVATE ${CLICKHOUSE_CPP_DIR} ${CLICKHOUSE_CPP_DIR}/contrib/absl)
target_include_directories(${LOADABLE_EXTENSION_NAME} PRIVATE ${CLICKHOUSE_CPP_DIR} ${CLICKHOUSE_CPP_DIR}/contrib/absl)
target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto clickhouse-cpp-lib)
target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto clickhouse-cpp-lib)
target_compile_features(${EXTENSION_NAME} PRIVATE cxx_std_17)
target_compile_features(${LOADABLE_EXTENSION_NAME} PRIVATE cxx_std_17)
