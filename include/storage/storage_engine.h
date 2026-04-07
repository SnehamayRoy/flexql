#ifndef FLEXQL_STORAGE_ENGINE_H
#define FLEXQL_STORAGE_ENGINE_H

#include "buffer_pool.h"
#include "query_cache.h"
#include "types.h"
#include <filesystem>
#include <memory>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <functional>

struct Database {
    std::unordered_map<std::string, std::shared_ptr<Table>> tables;
    mutable std::shared_mutex mutex;
    QueryCache cache;
    BufferPool pool{1024}; // Manage 4096 pages (16MB) or 1024 (4MB)
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
bool initialize_table_file(Table& table, std::string& error);
bool rewrite_catalog(const Database& database, std::string& error);

// New Page-based operations
bool persist_rows_batch(Database& database, Table& table, const std::vector<Row>& rows, const std::vector<std::string>& pk_strings, std::string& error);
void scan_table(Database& database, Table& tbl, std::function<void(const Row&)> callback);
bool fetch_row_by_id(Database& database, Table& tbl, size_t index_val, Row& row);
void deserialize_row(const char* data, size_t& off, size_t end, const std::vector<Column>& columns, Row& row);
size_t encode_record_id(PageId pid, SlotId sid);
void decode_record_id(size_t val, PageId& pid, SlotId& sid);

bool load_database(Database& database, std::string& error);
std::shared_ptr<Table> find_table(const Database& database, const std::string& name);

#endif
