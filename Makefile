CXX = g++
CXXFLAGS = -std=c++17 -O3 -pthread
INCLUDES = -I./include/common -I./include/index -I./include/cache -I./include/concurrency -I./include/parser -I./include/storage -I./include/query -I./include/utils

SERVER_SRC = src/server/flexql_server.cpp src/concurrency/thread_pool.cpp src/cache/query_cache.cpp src/parser/sql_parser.cpp src/storage/storage_engine.cpp src/storage/buffer_pool.cpp src/query/executor.cpp src/utils/sql_utils.cpp src/index/bplustree.cpp
CLIENT_SRC = src/client/flexql.cpp

all: bin/server bin/benchmark bin/flexql_cli bin/my_benchmark bin/compat_matrix_benchmark

bin:
	mkdir -p bin

bin/server: bin $(SERVER_SRC)
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(SERVER_SRC) -o bin/server

bin/benchmark: bin $(CLIENT_SRC) tests/benchmark_flexql.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(CLIENT_SRC) tests/benchmark_flexql.cpp -o bin/benchmark

bin/flexql_cli: bin $(CLIENT_SRC) src/client/flexql_cli.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(CLIENT_SRC) src/client/flexql_cli.cpp -o bin/flexql_cli

bin/my_benchmark: bin $(CLIENT_SRC) my_benchmark.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(CLIENT_SRC) my_benchmark.cpp -o bin/my_benchmark

bin/compat_matrix_benchmark: bin $(CLIENT_SRC) compat_matrix_benchmark.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) $(CLIENT_SRC) compat_matrix_benchmark.cpp -o bin/compat_matrix_benchmark

clean:
	rm -rf bin/* data/*
