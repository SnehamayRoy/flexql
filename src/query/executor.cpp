#include "executor.h"

#include "sql_parser.h"
#include "sql_utils.h"

#include <cctype>
#include <unordered_map>
#include <unordered_set>
#include <cstring>
#include <optional>
#include <string_view>

namespace {

bool starts_with_ci(std::string_view text, std::string_view prefix) {
    if (text.size() < prefix.size()) {
        return false;
    }
    for (size_t i = 0; i < prefix.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(text[i])) !=
            std::tolower(static_cast<unsigned char>(prefix[i]))) {
            return false;
        }
    }
    return true;
}

size_t find_ci(std::string_view haystack, std::string_view needle) {
    if (needle.empty()) {
        return 0;
    }
    if (haystack.size() < needle.size()) {
        return std::string::npos;
    }
    for (size_t i = 0; i + needle.size() <= haystack.size(); ++i) {
        bool match = true;
        for (size_t j = 0; j < needle.size(); ++j) {
            if (std::tolower(static_cast<unsigned char>(haystack[i + j])) !=
                std::tolower(static_cast<unsigned char>(needle[j]))) {
                match = false;
                break;
            }
        }
        if (match) {
            return i;
        }
    }
    return std::string::npos;
}

std::optional<Row::Value> resolve_operand(
    const Operand &operand,
    const Table &left_table,
    const Row &left_row,
    const Table *right_table,
    const Row *right_row) {
    if (operand.is_literal) {
        return Row::Value{operand.literal};
    }

    auto resolve_from_table = [&](const Table &table, const Row &row) -> std::optional<Row::Value> {
        auto idx_it = table.column_index.find(operand.column_name);
        if (idx_it == table.column_index.end()) {
            return std::nullopt;
        }
        return row.values[idx_it->second];
    };

    if (!operand.table_name.empty()) {
        if (iequals(operand.table_name, left_table.name)) {
            return resolve_from_table(left_table, left_row);
        }
        if (right_table && right_row && iequals(operand.table_name, right_table->name)) {
            return resolve_from_table(*right_table, *right_row);
        }
        return std::nullopt;
    }

    auto left_value = resolve_from_table(left_table, left_row);
    if (left_value.has_value()) {
        return left_value;
    }
    if (right_table && right_row) {
        return resolve_from_table(*right_table, *right_row);
    }
    return std::nullopt;
}

bool evaluate_condition(
    const Condition &condition,
    const Table &left_table,
    const Row &left_row,
    const Table *right_table,
    const Row *right_row) {
    auto left = resolve_operand(condition.left, left_table, left_row, right_table, right_row);
    auto right = resolve_operand(condition.right, left_table, left_row, right_table, right_row);
    if (!left.has_value() || !right.has_value()) {
        return false;
    }
    return evaluate_comparison(left.value(), condition.op, right.value());
}

bool resolve_select_item(
    const std::string &item,
    const Table &left_table,
    const Row &left_row,
    const Table *right_table,
    const Row *right_row,
    std::string &column_name,
    std::string &value) {
    Operand operand;
    size_t dot = item.find('.');
    if (dot != std::string::npos) {
        operand.table_name = trim(item.substr(0, dot));
        operand.column_name = trim(item.substr(dot + 1));
        column_name = trim(item);
    } else {
        operand.column_name = trim(item);
        column_name = trim(item);
    }

    auto resolved = resolve_operand(operand, left_table, left_row, right_table, right_row);
    if (!resolved.has_value()) {
        return false;
    }
    value = value_to_string(resolved.value());
    return true;
}

std::optional<Row::Value> resolve_table_operand(
    const Operand &operand,
    const Table &table,
    const Row &row) {
    if (operand.is_literal) {
        return Row::Value{operand.literal};
    }
    if (!operand.table_name.empty() && !iequals(operand.table_name, table.name)) {
        return std::nullopt;
    }
    auto idx_it = table.column_index.find(operand.column_name);
    if (idx_it == table.column_index.end()) {
        return std::nullopt;
    }
    return row.values[idx_it->second];
}

struct JoinKeyAccess {
    const Operand *left_operand = nullptr;
    const Operand *right_operand = nullptr;
};

std::optional<JoinKeyAccess> extract_hash_join_access(
    const Condition &condition,
    const Table &left_table,
    const Table &right_table) {
    if (condition.op != "=" || condition.left.is_literal || condition.right.is_literal) {
        return std::nullopt;
    }

    auto belongs_to = [](const Operand &operand, const Table &table) {
        return !operand.table_name.empty() && iequals(operand.table_name, table.name);
    };

    if (belongs_to(condition.left, left_table) && belongs_to(condition.right, right_table)) {
        return JoinKeyAccess{&condition.left, &condition.right};
    }
    if (belongs_to(condition.left, right_table) && belongs_to(condition.right, left_table)) {
        return JoinKeyAccess{&condition.right, &condition.left};
    }
    return std::nullopt;
}

struct PrimaryIndexFilter {
    enum class Type {
        None,
        Equal,
        Greater,
        GreaterEqual,
        Less,
        LessEqual
    };

    Type type = Type::None;
    std::string literal;
};

PrimaryIndexFilter extract_primary_index_filter(const Table &table, const std::optional<Condition> &condition) {
    if (!condition.has_value() || table.columns.empty()) {
        return {};
    }

    const Condition &c = condition.value();
    const std::string &pk_name = table.columns.front().name;

    auto matches_primary = [&](const Operand &operand) {
        return !operand.is_literal &&
               (operand.table_name.empty() || iequals(operand.table_name, table.name)) &&
               iequals(operand.column_name, pk_name);
    };

    if (matches_primary(c.left) && c.right.is_literal) {
        if (c.op == "=") {
            return {PrimaryIndexFilter::Type::Equal, c.right.literal};
        }
        if (c.op == ">") {
            return {PrimaryIndexFilter::Type::Greater, c.right.literal};
        }
        if (c.op == ">=") {
            return {PrimaryIndexFilter::Type::GreaterEqual, c.right.literal};
        }
        if (c.op == "<") {
            return {PrimaryIndexFilter::Type::Less, c.right.literal};
        }
        if (c.op == "<=") {
            return {PrimaryIndexFilter::Type::LessEqual, c.right.literal};
        }
    }

    if (c.left.is_literal && matches_primary(c.right)) {
        if (c.op == "=") {
            return {PrimaryIndexFilter::Type::Equal, c.left.literal};
        }
        if (c.op == "<=") {
            return {PrimaryIndexFilter::Type::GreaterEqual, c.left.literal};
        }
        if (c.op == "<") {
            return {PrimaryIndexFilter::Type::Greater, c.left.literal};
        }
        if (c.op == ">=") {
            return {PrimaryIndexFilter::Type::LessEqual, c.left.literal};
        }
        if (c.op == ">") {
            return {PrimaryIndexFilter::Type::Less, c.left.literal};
        }
    }

    return {};
}

bool should_use_fast_insert_path(const std::string &sql) {
    // Always use the fast path for any INSERT INTO ... VALUES (...)
    // It handles both single-row and multi-row batches correctly
    (void)sql;
    return true;
}

bool fast_parse_insert_rows(const std::string &sql,
                            const Table &table,
                            std::vector<Row> &rows,
                            std::string &error) {
    // Fast zero-copy parser: works directly on sql without making an uppercase copy.
    // Finds VALUES case-insensitively, then parses tuples in one pass.
    const char* p   = sql.data();
    const char* end = p + sql.size();

    // Skip "INSERT INTO <table> VALUES" prefix (case-insensitive VALUES search)
    // Find the first '(' which starts the first tuple
    const char* body = nullptr;
    for (const char* q = p; q < end - 1; ++q) {
        if ((*q == 'V' || *q == 'v') &&
            (q+6 < end) &&
            (q[1]=='A'||q[1]=='a') && (q[2]=='L'||q[2]=='l') &&
            (q[3]=='U'||q[3]=='u') && (q[4]=='E'||q[4]=='e') &&
            (q[5]=='S'||q[5]=='s')) {
            // skip past VALUES and whitespace
            q += 6;
            while (q < end && (unsigned char)*q <= ' ') ++q;
            body = q;
            break;
        }
    }
    if (!body) { error = "invalid INSERT syntax"; return false; }

    const size_t ncols = table.columns.size();
    rows.clear();
    rows.reserve(64);
    p = body;

    while (p < end) {
        // skip whitespace and commas between tuples
        while (p < end && ((unsigned char)*p <= ' ' || *p == ',')) ++p;
        if (p >= end) break;
        if (*p != '(') { error = "expected ( in INSERT VALUES"; return false; }
        ++p;

        Row row;
        row.values.reserve(ncols);
        size_t col = 0;

        while (p < end && *p != ')') {
            // skip leading whitespace
            while (p < end && (unsigned char)*p <= ' ') ++p;

            if (*p == '\'') {
                // quoted string with backslash-escaped quotes
                ++p;
                std::string literal;
                literal.reserve(32);
                bool closed = false;
                while (p < end) {
                    if (*p == '\\' && p + 1 < end) {
                        ++p;
                        literal.push_back(*p);
                        ++p;
                        continue;
                    }
                    if (*p == '\'') {
                        ++p;
                        closed = true;
                        break;
                    }
                    literal.push_back(*p);
                    ++p;
                }
                if (!closed) {
                    error = "unterminated quoted string in INSERT";
                    return false;
                }
                row.values.emplace_back(std::move(literal));
            } else {
                // unquoted token (number)
                const char* start = p;
                while (p < end && *p != ',' && *p != ')') ++p;
                // trim trailing whitespace
                const char* tok_end = p;
                while (tok_end > start && (unsigned char)*(tok_end-1) <= ' ') --tok_end;
                std::string_view tok(start, tok_end - start);

                if (col < ncols) {
                    ColumnType ct = table.columns[col].type;
                    if (ct == ColumnType::Decimal) {
                        char tbuf[64];
                        size_t len = std::min<size_t>(tok.size(), 63);
                        memcpy(tbuf, tok.data(), len);
                        tbuf[len] = '\0';
                        char* end;
                        double val = std::strtod(tbuf, &end);
                        if (end == tbuf) {
                            row.values.emplace_back(std::string(tok));
                        } else {
                            row.values.emplace_back(val);
                        }
                    } else if (ct == ColumnType::Int || ct == ColumnType::DateTime) {
                        int64_t val = 0;
                        int sign = 1;
                        size_t i = 0;
                        if (tok.size() > 0 && tok[0] == '-') { sign = -1; i++; }
                        for (; i < tok.size(); ++i) {
                            if (tok[i] >= '0' && tok[i] <= '9') {
                                val = val * 10 + (tok[i] - '0');
                            } else {
                                break; 
                            }
                        }
                        row.values.emplace_back(val * sign);
                    } else {
                        row.values.emplace_back(std::string(tok));
                    }
                } else {
                    row.values.emplace_back(std::string(tok));
                }
            }
            ++col;
            // skip comma between fields
            while (p < end && (unsigned char)*p <= ' ') ++p;
            if (p < end && *p == ',') ++p;
        }
        if (p < end && *p == ')') ++p;

        if (row.values.size() != ncols) {
            error = "column count mismatch for table: " + table.name;
            return false;
        }
        rows.push_back(std::move(row));
    }

    if (rows.empty()) { error = "empty INSERT"; return false; }
    return true;
}

ExecResult execute_create_table(Database &database, const std::string &sql) {
    ExecResult result;
    std::string table_name;
    std::vector<Column> columns;
    std::string error;
    if (!parse_create_table(sql, table_name, columns, error)) {
        result.error = error;
        return result;
    }

    auto table = std::make_shared<Table>();
    table->name = table_name;
    table->columns = columns;
    table->sync_every_batches = database.sync_every_batches;
    for (size_t i = 0; i < columns.size(); ++i) {
        table->column_index[columns[i].name] = i;
    }
    table->primary_index = std::make_unique<BPlusTree>(
        16,
        (columns.front().type == ColumnType::Varchar) ? BPlusTree::Mode::Lexicographic
                                                      : BPlusTree::Mode::Numeric);

    {
        std::unique_lock<std::shared_mutex> lock(database.mutex);
        if (database.tables.find(table_name) != database.tables.end()) {
            result.error = "table already exists: " + table_name;
            return result;
        }
        database.tables[table_name] = table;
        if (!rewrite_catalog(database, error)) {
            database.tables.erase(table_name);
            result.error = error;
            return result;
        }
        if (!open_table_fd(*table, error)) {
            database.tables.erase(table_name);
            result.error = error;
            return result;
        }
    }

    result.ok = true;
    return result;
}

ExecResult execute_insert(Database &database, const std::string &sql) {
    ExecResult result;
    std::string table_name;
    std::vector<Row> new_rows;
    std::string error;

    std::string clean_sql = trim(sql);
    const std::string prefix = "INSERT INTO ";
    size_t values_pos = find_ci(clean_sql, " VALUES ");
    if (!starts_with_ci(clean_sql, prefix) || values_pos == std::string::npos) {
        result.error = "invalid INSERT syntax";
        return result;
    }
    table_name = trim(clean_sql.substr(prefix.size(), values_pos - prefix.size()));
    if (table_name.empty()) {
        result.error = "missing table name in INSERT";
        return result;
    }

    auto table = find_table(database, table_name);
    if (!table) {
        result.error = "missing table: " + table_name;
        return result;
    }

    const bool use_fast_insert = should_use_fast_insert_path(clean_sql);
    if (use_fast_insert) {
        if (!fast_parse_insert_rows(clean_sql, *table, new_rows, error)) {
            result.error = error;
            return result;
        }
    } else {
        if (!parse_insert(clean_sql, table_name, new_rows, error)) {
            result.error = error;
            return result;
        }
    }

    // Coerce values outside the lock (type conversion is CPU-bound, not data-race-sensitive)
    if (!use_fast_insert) {
        for (auto &row : new_rows) {
            if (!coerce_row(table->columns, row)) {
                result.error = "failed to coerce row values for table: " + table_name;
                return result;
            }
        }
    }

    // Pre-compute primary keys before acquiring the lock
    std::vector<std::string> pk_strings;
    pk_strings.reserve(new_rows.size());
    std::unordered_set<std::string> batch_keys;
    batch_keys.reserve(new_rows.size());
    for (const auto &row : new_rows) {
        if (row.values.size() != table->columns.size()) {
            result.error = "column count mismatch for table: " + table_name;
            return result;
        }
        std::string pk = value_to_string(row.values[0]);
        if (!batch_keys.insert(pk).second) {
            result.error = "duplicate primary key in batch: " + pk;
            return result;
        }
        pk_strings.push_back(std::move(pk));
    }

    std::unique_lock<std::shared_mutex> lock(table->mutex);

    // Validate PK uniqueness
    for (const auto &pk : pk_strings) {
        if (table->primary_index->contains(pk)) {
            result.error = "duplicate primary key: " + pk;
            return result;
        }
    }

    // Persist to disk and insert into primary index
    if (!persist_rows_batch(database, *table, new_rows, pk_strings, error)) {
        result.error = error;
        return result;
    }

    ++table->version;
    result.ok = true;
    return result;
}

ExecResult execute_select(Database &database, const std::string &sql) {
    ExecResult result;
    result.has_rows = true;

    auto cached = database.cache.get(sql, [&](const std::string &table_name) -> std::optional<uint64_t> {
        auto table = find_table(database, table_name);
        if (!table) {
            return std::nullopt;
        }
        std::shared_lock<std::shared_mutex> lock(table->mutex);
        return table->version;
    });
    if (cached.has_value()) {
        result.ok = true;
        result.query_result = cached.value();
        return result;
    }

    SelectQuery query;
    std::string error;
    if (!parse_select(sql, query, error)) {
        result.error = error;
        return result;
    }

    auto left_table = find_table(database, query.table_name);
    if (!left_table) {
        result.error = "missing table: " + query.table_name;
        return result;
    }

    std::shared_ptr<Table> right_table;
    if (query.join_table_name.has_value()) {
        right_table = find_table(database, query.join_table_name.value());
        if (!right_table) {
            result.error = "missing table: " + query.join_table_name.value();
            return result;
        }
    }

    std::shared_lock<std::shared_mutex> left_lock(left_table->mutex);
    std::optional<std::shared_lock<std::shared_mutex>> right_lock;
    if (right_table) {
        right_lock.emplace(right_table->mutex);
    }

    QueryResult query_result;
    auto fill_projection = [&](const Row &left_row, const Row *right_row) -> bool {
        std::vector<std::string> out_row;
        if (query.select_items.size() == 1 && query.select_items[0] == "*") {
            if (query_result.columns.empty()) {
                for (const auto &column : left_table->columns) {
                    query_result.columns.push_back(column.name);
                }
                if (right_table) {
                    for (const auto &column : right_table->columns) {
                        query_result.columns.push_back(column.name);
                    }
                }
            }
            for (const auto &value : left_row.values) {
                out_row.push_back(value_to_string(value));
            }
            if (right_row) {
                for (const auto &value : right_row->values) {
                    out_row.push_back(value_to_string(value));
                }
            }
        } else {
            for (const std::string &item : query.select_items) {
                std::string column_name;
                std::string value;
                if (!resolve_select_item(item, *left_table, left_row, right_table.get(), right_row, column_name, value)) {
                    return false;
                }
                if (query_result.columns.size() < query.select_items.size()) {
                    query_result.columns.push_back(column_name);
                }
                out_row.push_back(value);
            }
        }
        query_result.rows.push_back(std::move(out_row));
        return true;
    };

    if (!right_table) {
        // Ordered-index fast path for equality and range queries on primary key
        PrimaryIndexFilter index_filter = extract_primary_index_filter(*left_table, query.where_condition);
        bool used_index = false;
        if (index_filter.type != PrimaryIndexFilter::Type::None) {
            std::vector<size_t> row_indexes;
            if (index_filter.type == PrimaryIndexFilter::Type::Equal) {
                auto it = left_table->primary_index->find(index_filter.literal);
                if (it.has_value()) {
                    row_indexes.push_back(it.value());
                }
            } else if (index_filter.type == PrimaryIndexFilter::Type::Greater) {
                row_indexes = left_table->primary_index->scan_greater(index_filter.literal, false);
            } else if (index_filter.type == PrimaryIndexFilter::Type::GreaterEqual) {
                row_indexes = left_table->primary_index->scan_greater(index_filter.literal, true);
            } else if (index_filter.type == PrimaryIndexFilter::Type::Less) {
                row_indexes = left_table->primary_index->scan_less(index_filter.literal);
            } else if (index_filter.type == PrimaryIndexFilter::Type::LessEqual) {
                row_indexes = left_table->primary_index->scan_less_equal(index_filter.literal);
            }

            for (size_t row_index : row_indexes) {
                Row row;
                if (!fetch_row_by_id(database, *left_table, row_index, row)) continue;
                if (!query.where_condition.has_value() ||
                    evaluate_condition(query.where_condition.value(), *left_table, row, nullptr, nullptr)) {
                    if (!fill_projection(row, nullptr)) {
                        result.error = "unknown column in SELECT";
                        return result;
                    }
                }
            }
            used_index = true;
        }

        // Sequential scan for non-indexed WHERE
        if (!used_index) {
            bool has_error = false;
            scan_table(database, *left_table, [&](const Row& row) {
                if (has_error) return;
                if (query.where_condition.has_value() &&
                    !evaluate_condition(query.where_condition.value(), *left_table, row, nullptr, nullptr)) {
                    return;
                }
                if (!fill_projection(row, nullptr)) {
                    result.error = "unknown column in SELECT";
                    has_error = true;
                }
            });
            if (has_error) return result;
        }
    } else {
        auto join_access = extract_hash_join_access(query.join_condition.value(), *left_table, *right_table);
        if (join_access.has_value()) {
            bool join_error = false;
            std::unordered_map<std::string, std::vector<Row>> right_rows_by_key;

            scan_table(database, *right_table, [&](const Row& right_row) {
                if (join_error) return;
                auto right_key = resolve_table_operand(*join_access->right_operand, *right_table, right_row);
                if (!right_key.has_value()) {
                    result.error = "unknown column in JOIN";
                    join_error = true;
                    return;
                }
                right_rows_by_key[value_to_string(right_key.value())].push_back(right_row);
            });
            if (join_error) return result;

            scan_table(database, *left_table, [&](const Row& left_row) {
                if (join_error) return;
                auto left_key = resolve_table_operand(*join_access->left_operand, *left_table, left_row);
                if (!left_key.has_value()) {
                    result.error = "unknown column in JOIN";
                    join_error = true;
                    return;
                }
                auto matches = right_rows_by_key.find(value_to_string(left_key.value()));
                if (matches == right_rows_by_key.end()) {
                    return;
                }
                for (const Row &right_row : matches->second) {
                    if (query.where_condition.has_value() &&
                        !evaluate_condition(query.where_condition.value(), *left_table, left_row, right_table.get(), &right_row)) {
                        continue;
                    }
                    if (!fill_projection(left_row, &right_row)) {
                        result.error = "unknown column in SELECT";
                        join_error = true;
                        return;
                    }
                }
            });
            if (join_error) return result;

        } else {
            bool nl_error = false;
            scan_table(database, *left_table, [&](const Row& left_row) {
                if (nl_error) return;
                scan_table(database, *right_table, [&](const Row& right_row) {
                    if (nl_error) return;
                    if (!evaluate_condition(query.join_condition.value(), *left_table, left_row, right_table.get(), &right_row)) {
                        return;
                    }
                    if (query.where_condition.has_value() &&
                        !evaluate_condition(query.where_condition.value(), *left_table, left_row, right_table.get(), &right_row)) {
                        return;
                    }
                    if (!fill_projection(left_row, &right_row)) {
                        result.error = "unknown column in SELECT";
                        nl_error = true;
                    }
                });
            });
            if (nl_error) return result;
        }
    }

    CachedResult cached_result;
    cached_result.result = query_result;
    cached_result.table_names.push_back(left_table->name);
    cached_result.versions.push_back(left_table->version);
    if (right_table) {
        cached_result.table_names.push_back(right_table->name);
        cached_result.versions.push_back(right_table->version);
    }
    database.cache.put(sql, std::move(cached_result));

    result.ok = true;
    result.query_result = std::move(query_result);
    return result;
}

}  // namespace

ExecResult execute_sql(Database &database, const std::string &sql) {
    std::string clean_sql = strip_trailing_semicolon(sql);
    if (starts_with_ci(clean_sql, "CREATE TABLE")) {
        return execute_create_table(database, clean_sql);
    }
    if (starts_with_ci(clean_sql, "INSERT INTO")) {
        return execute_insert(database, clean_sql);
    }
    if (starts_with_ci(clean_sql, "SELECT")) {
        return execute_select(database, clean_sql);
    }

    ExecResult result;
    result.error = "unsupported SQL statement";
    return result;
}
