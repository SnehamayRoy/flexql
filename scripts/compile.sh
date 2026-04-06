#!/usr/bin/env sh
set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT_DIR/include/concurrency" "$ROOT_DIR/src/concurrency"

g++ -std=c++17 -O3 -pthread \
  -I"$ROOT_DIR/include/common" \
  -I"$ROOT_DIR/include/index" \
  -I"$ROOT_DIR/include/cache" \
  -I"$ROOT_DIR/include/concurrency" \
  -I"$ROOT_DIR/include/parser" \
  -I"$ROOT_DIR/include/storage" \
  -I"$ROOT_DIR/include/query" \
  -I"$ROOT_DIR/include/utils" \
  "$ROOT_DIR/src/server/flexql_server.cpp" \
  "$ROOT_DIR/src/concurrency/thread_pool.cpp" \
  "$ROOT_DIR/src/cache/query_cache.cpp" \
  "$ROOT_DIR/src/parser/sql_parser.cpp" \
  "$ROOT_DIR/src/storage/storage_engine.cpp" \
  "$ROOT_DIR/src/query/executor.cpp" \
  "$ROOT_DIR/src/utils/sql_utils.cpp" \
  "$ROOT_DIR/src/index/bplustree.cpp" \
  -o "$ROOT_DIR/bin/server"

g++ -std=c++17 -O3 -pthread \
  -I"$ROOT_DIR/include/common" \
  -I"$ROOT_DIR/include/utils" \
  "$ROOT_DIR/src/client/flexql.cpp" \
  "$ROOT_DIR/tests/benchmark_flexql.cpp" \
  -o "$ROOT_DIR/bin/benchmark"

g++ -std=c++17 -O3 -pthread \
  -I"$ROOT_DIR/include/common" \
  -I"$ROOT_DIR/include/utils" \
  "$ROOT_DIR/src/client/flexql.cpp" \
  "$ROOT_DIR/src/client/flexql_cli.cpp" \
  -o "$ROOT_DIR/bin/flexql_cli"
