#include "sql_parser.h"

#include "sql_utils.h"

#include <regex>
#include <sstream>
#include <stdexcept>
#include <vector>

ColumnType parse_column_type(const std::string &token) {
    std::string upper = to_upper(trim(token));
    if (upper == "INT") {
        return ColumnType::Int;
    }
    if (upper == "DECIMAL") {
        return ColumnType::Decimal;
    }
    if (upper.rfind("VARCHAR", 0) == 0) {
        return ColumnType::Varchar;
    }
    if (upper == "DATETIME") {
        return ColumnType::DateTime;
    }
    throw std::runtime_error("unsupported column type: " + token);
}

std::string column_type_name(ColumnType type) {
    switch (type) {
        case ColumnType::Int: return "INT";
        case ColumnType::Decimal: return "DECIMAL";
        case ColumnType::Varchar: return "VARCHAR";
        case ColumnType::DateTime: return "DATETIME";
    }
    return "VARCHAR";
}

std::optional<Operand> parse_operand(const std::string &token) {
    std::string cleaned = trim(token);
    if (cleaned.empty()) {
        return std::nullopt;
    }

    Operand operand;
    if (cleaned.front() == '\'' && cleaned.back() == '\'') {
        operand.is_literal = true;
        operand.literal = unquote(cleaned);
        return operand;
    }
    if (is_number(cleaned)) {
        operand.is_literal = true;
        operand.literal = cleaned;
        return operand;
    }

    size_t dot = cleaned.find('.');
    if (dot != std::string::npos) {
        operand.table_name = trim(cleaned.substr(0, dot));
        operand.column_name = trim(cleaned.substr(dot + 1));
    } else {
        operand.column_name = cleaned;
    }
    return operand;
}

std::optional<Condition> parse_condition(const std::string &text) {
    static const std::vector<std::string> ops = {">=", "<=", "=", ">", "<"};
    bool in_quotes = false;
    for (size_t i = 0; i < text.size(); ++i) {
        if (text[i] == '\'' && (i == 0 || text[i - 1] != '\\')) {
            in_quotes = !in_quotes;
            continue;
        }
        if (in_quotes) {
            continue;
        }
        for (const std::string &op : ops) {
            if (text.compare(i, op.size(), op) == 0) {
                auto left = parse_operand(text.substr(0, i));
                auto right = parse_operand(text.substr(i + op.size()));
                if (!left.has_value() || !right.has_value()) {
                    return std::nullopt;
                }
                return Condition{left.value(), right.value(), op};
            }
        }
    }
    return std::nullopt;
}

bool parse_create_table(const std::string &sql, std::string &table_name, std::vector<Column> &columns, std::string &error) {
    static const std::regex pattern(
        R"(^\s*CREATE\s+TABLE\s+([A-Za-z_][A-Za-z0-9_]*)\s*\((.*)\)\s*$)",
        std::regex_constants::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, pattern)) {
        error = "invalid CREATE TABLE syntax";
        return false;
    }

    table_name = match[1];
    std::string body = match[2];
    for (const std::string &part : split_csv(body)) {
        std::stringstream ss(part);
        std::string column_name;
        std::string column_type;
        if (!(ss >> column_name >> column_type)) {
            error = "invalid column definition in CREATE TABLE";
            return false;
        }
        try {
            columns.push_back({column_name, parse_column_type(column_type)});
        } catch (const std::exception &ex) {
            error = ex.what();
            return false;
        }
    }

    if (columns.empty()) {
        error = "CREATE TABLE requires at least one column";
        return false;
    }
    return true;
}

bool parse_insert(const std::string &sql, std::string &table_name, std::vector<Row> &rows, std::string &error) {
    std::string clean = trim(sql);
    std::string upper = to_upper(clean);
    const std::string prefix = "INSERT INTO ";
    size_t values_pos = upper.find(" VALUES ");
    if (upper.rfind(prefix, 0) != 0 || values_pos == std::string::npos) {
        error = "invalid INSERT syntax";
        return false;
    }

    table_name = trim(clean.substr(prefix.size(), values_pos - prefix.size()));
    if (table_name.empty()) {
        error = "missing table name in INSERT";
        return false;
    }

    std::string body = trim(clean.substr(values_pos + 8));
    size_t pos = 0;
    while (pos < body.size()) {
        while (pos < body.size() && std::isspace(static_cast<unsigned char>(body[pos]))) {
            ++pos;
        }
        if (pos >= body.size() || body[pos] != '(') {
            error = "invalid row batch in INSERT";
            return false;
        }

        ++pos;
        bool in_quotes = false;
        std::string current;
        std::vector<std::string> fields;
        while (pos < body.size()) {
            char ch = body[pos];
            if (ch == '\'' && body[pos - 1] != '\\') {
                in_quotes = !in_quotes;
                current.push_back(ch);
                ++pos;
                continue;
            }
            if (ch == ',' && !in_quotes) {
                fields.push_back(unquote(trim(current)));
                current.clear();
                ++pos;
                continue;
            }
            if (ch == ')' && !in_quotes) {
                fields.push_back(unquote(trim(current)));
                current.clear();
                ++pos;
                break;
            }
            current.push_back(ch);
            ++pos;
        }
        if (fields.empty()) {
            error = "empty row in INSERT";
            return false;
        }
        Row row;
        row.values.reserve(fields.size());
        for (std::string &field : fields) {
            row.values.emplace_back(std::move(field));
        }
        rows.push_back(std::move(row));

        while (pos < body.size() && std::isspace(static_cast<unsigned char>(body[pos]))) {
            ++pos;
        }
        if (pos < body.size()) {
            if (body[pos] != ',') {
                error = "invalid INSERT row separator";
                return false;
            }
            ++pos;
        }
    }

    return !rows.empty();
}

bool parse_select(const std::string &sql, SelectQuery &query, std::string &error) {
    static const std::regex pattern(
        R"(^\s*SELECT\s+(.+?)\s+FROM\s+([A-Za-z_][A-Za-z0-9_]*)(?:\s+INNER\s+JOIN\s+([A-Za-z_][A-Za-z0-9_]*)\s+ON\s+(.+?))?(?:\s+WHERE\s+(.+))?\s*$)",
        std::regex_constants::icase);
    std::smatch match;
    if (!std::regex_match(sql, match, pattern)) {
        error = "invalid SELECT syntax";
        return false;
    }

    query.select_items = split_csv(match[1]);
    query.table_name = match[2];

    if (match[3].matched) {
        query.join_table_name = match[3].str();
        auto join_condition = parse_condition(match[4]);
        if (!join_condition.has_value()) {
            error = "invalid JOIN condition";
            return false;
        }
        query.join_condition = join_condition;
    }

    if (match[5].matched) {
        auto where_condition = parse_condition(match[5]);
        if (!where_condition.has_value()) {
            error = "invalid WHERE condition";
            return false;
        }
        query.where_condition = where_condition;
    }
    return true;
}
