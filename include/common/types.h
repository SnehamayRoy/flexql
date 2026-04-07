#ifndef FLEXQL_COMMON_TYPES_H
#define FLEXQL_COMMON_TYPES_H

#include "bplustree.h"

#include <memory>
#include <cstdint>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

enum class ColumnType { Int, Decimal, Varchar, DateTime };

struct Column {
    std::string name;
    ColumnType  type;
};

struct Row {
    using Value = std::variant<int64_t, double, std::string>;
    std::vector<Value> values;
};

struct Operand {
    bool is_literal = false;
    std::string literal;
    std::string table_name;
    std::string column_name;
};

struct Condition {
    Operand left, right;
    std::string op;
};

struct SelectQuery {
    std::vector<std::string> select_items;
    std::string table_name;
    std::optional<std::string> join_table_name;
    std::optional<Condition> join_condition;
    std::optional<Condition> where_condition;
};

struct QueryResult {
    std::vector<std::string> columns;
    std::vector<std::vector<std::string>> rows;
};

struct ExecResult {
    bool ok = false;
    std::string error;
    QueryResult query_result;
    bool has_rows = false;
};

struct Table {
    std::string name;
    std::vector<Column> columns;
    std::unordered_map<std::string, size_t>  column_index;
    uint32_t next_page_id = 1;
    uint32_t next_slot_id = 0;
    // Primary index on the first column.
    std::unique_ptr<BPlusTree> primary_index;

    // ── Disk append fd ────────────────────────────────────────────────────────
    int    row_fd             = -1;
    size_t sync_every_batches = 1;
    size_t batches_since_sync = 0;

    // ── Version for query-result cache invalidation ───────────────────────────
    uint64_t version = 0;

    mutable std::shared_mutex mutex;
};

#endif
