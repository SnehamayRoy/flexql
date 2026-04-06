#ifndef FLEXQL_STORAGE_ENGINE_H
#define FLEXQL_STORAGE_ENGINE_H

#include "query_cache.h"
#include "types.h"

#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>

struct Database {
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;
    mutable std::shared_mutex mutex;
    QueryCache cache;
    size_t sync_every_batches = 1;
    bool use_wal = false;
    int  wal_fd  = -1;
    size_t wal_batches_since_sync = 0;
};

std::filesystem::path storage_root_dir();
std::filesystem::path table_data_path(const std::string& table_name);
std::filesystem::path wal_file_path();
bool ensure_store_dir();
bool open_table_fd(Table& table, std::string& error);
void close_table_fd(Table& table);
bool rewrite_catalog(const Database& database, std::string& error);
bool append_rows_to_disk(Table& table, const std::vector<Row>& rows, std::string& error);
bool persist_rows(Database& database, Table& table, const std::vector<Row>& rows, std::string& error);
bool load_database(Database& database, std::string& error);
std::shared_ptr<Table> find_table(const Database& database, const std::string& name);

#endif
