#include "sql_utils.h"

#include <algorithm>
#include <cerrno>
#include <cctype>
#include <cstdlib>
#include <iomanip>
#include <sstream>

namespace {

bool value_to_number(const Row::Value &value, long double &out) {
    if (std::holds_alternative<int64_t>(value)) {
        out = static_cast<long double>(std::get<int64_t>(value));
        return true;
    }
    if (std::holds_alternative<double>(value)) {
        out = static_cast<long double>(std::get<double>(value));
        return true;
    }
    return parse_number(std::get<std::string>(value), out);
}

}  // namespace

std::string trim(const std::string &value) {
    size_t start = 0;
    while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) {
        ++start;
    }
    size_t end = value.size();
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) {
        --end;
    }
    return value.substr(start, end - start);
}

std::string strip_trailing_semicolon(const std::string &sql) {
    std::string out = trim(sql);
    while (!out.empty() && out.back() == ';') {
        out.pop_back();
        out = trim(out);
    }
    return out;
}

std::string to_upper(std::string input) {
    std::transform(input.begin(), input.end(), input.begin(), [](unsigned char ch) {
        return static_cast<char>(std::toupper(ch));
    });
    return input;
}

bool iequals(const std::string &a, const std::string &b) {
    if (a.size() != b.size()) {
        return false;
    }
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::toupper(static_cast<unsigned char>(a[i])) !=
            std::toupper(static_cast<unsigned char>(b[i]))) {
            return false;
        }
    }
    return true;
}

bool is_number(const std::string &value) {
    if (value.empty()) {
        return false;
    }
    char *end = nullptr;
    errno = 0;
    std::strtold(value.c_str(), &end);
    return errno == 0 && end && *end == '\0';
}

bool parse_number(const std::string &value, long double &out) {
    if (!is_number(value)) {
        return false;
    }
    out = std::strtold(value.c_str(), nullptr);
    return true;
}

std::vector<std::string> split_csv(const std::string &input) {
    std::vector<std::string> parts;
    std::string current;
    bool in_quotes = false;
    for (size_t i = 0; i < input.size(); ++i) {
        char ch = input[i];
        if (ch == '\'' && (i == 0 || input[i - 1] != '\\')) {
            in_quotes = !in_quotes;
            current.push_back(ch);
            continue;
        }
        if (ch == ',' && !in_quotes) {
            parts.push_back(trim(current));
            current.clear();
            continue;
        }
        current.push_back(ch);
    }
    if (!current.empty()) {
        parts.push_back(trim(current));
    }
    return parts;
}

std::string unquote(const std::string &value) {
    if (value.size() >= 2 && value.front() == '\'' && value.back() == '\'') {
        std::string out;
        out.reserve(value.size() - 2);
        for (size_t i = 1; i + 1 < value.size(); ++i) {
            if (value[i] == '\\' && i + 1 < value.size() - 1) {
                ++i;
                out.push_back(value[i]);
            } else {
                out.push_back(value[i]);
            }
        }
        return out;
    }
    return trim(value);
}

bool evaluate_comparison(const std::string &left, const std::string &op, const std::string &right) {
    long double left_num = 0;
    long double right_num = 0;
    bool numeric = parse_number(left, left_num) && parse_number(right, right_num);

    if (op == "=") {
        return numeric ? left_num == right_num : left == right;
    }
    if (op == ">") {
        return numeric ? left_num > right_num : left > right;
    }
    if (op == "<") {
        return numeric ? left_num < right_num : left < right;
    }
    if (op == ">=") {
        return numeric ? left_num >= right_num : left >= right;
    }
    if (op == "<=") {
        return numeric ? left_num <= right_num : left <= right;
    }
    return false;
}

bool evaluate_comparison(const Row::Value &left, const std::string &op, const Row::Value &right) {
    long double left_num = 0;
    long double right_num = 0;
    bool numeric = value_to_number(left, left_num) && value_to_number(right, right_num);
    if (numeric) {
        if (op == "=") {
            return left_num == right_num;
        }
        if (op == ">") {
            return left_num > right_num;
        }
        if (op == "<") {
            return left_num < right_num;
        }
        if (op == ">=") {
            return left_num >= right_num;
        }
        if (op == "<=") {
            return left_num <= right_num;
        }
        return false;
    }
    return evaluate_comparison(value_to_string(left), op, value_to_string(right));
}

std::string format_decimal(double value) {
    // Use snprintf — 3-4x faster than ostringstream
    char buf[64];
    int n = snprintf(buf, sizeof(buf), "%.15g", value);
    if (n <= 0) return "0";
    return std::string(buf, static_cast<size_t>(n));
}

std::string value_to_string(const Row::Value &value) {
    if (std::holds_alternative<int64_t>(value)) {
        return std::to_string(std::get<int64_t>(value));
    }
    if (std::holds_alternative<double>(value)) {
        return format_decimal(std::get<double>(value));
    }
    return std::get<std::string>(value);
}

bool coerce_value(ColumnType type, const Row::Value &input, Row::Value &output) {
    try {
        switch (type) {
            case ColumnType::Varchar:
                output = value_to_string(input);
                return true;
            case ColumnType::Decimal: {
                if (std::holds_alternative<double>(input)) {
                    output = std::get<double>(input);
                    return true;
                }
                if (std::holds_alternative<int64_t>(input)) {
                    output = static_cast<double>(std::get<int64_t>(input));
                    return true;
                }
                output = std::stod(std::get<std::string>(input));
                return true;
            }
            case ColumnType::Int:
            case ColumnType::DateTime: {
                if (std::holds_alternative<int64_t>(input)) {
                    output = std::get<int64_t>(input);
                    return true;
                }
                if (std::holds_alternative<double>(input)) {
                    output = static_cast<int64_t>(std::get<double>(input));
                    return true;
                }
                output = std::stoll(std::get<std::string>(input));
                return true;
            }
        }
    } catch (const std::exception &) {
        return false;
    }
    return false;
}

bool coerce_row(const std::vector<Column> &columns, Row &row) {
    if (row.values.size() != columns.size()) {
        return false;
    }
    for (size_t i = 0; i < columns.size(); ++i) {
        Row::Value typed_value;
        if (!coerce_value(columns[i].type, row.values[i], typed_value)) {
            return false;
        }
        row.values[i] = std::move(typed_value);
    }
    return true;
}

void append_u32_le(std::string &out, uint32_t value) {
    for (int i = 0; i < 4; ++i) {
        out.push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

void append_u64_le(std::string &out, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out.push_back(static_cast<char>((value >> (i * 8)) & 0xff));
    }
}

bool read_u32_le(const std::string &buffer, size_t &offset, uint32_t &value) {
    if (offset + 4 > buffer.size()) {
        return false;
    }
    value = 0;
    for (int i = 0; i < 4; ++i) {
        value |= static_cast<uint32_t>(static_cast<unsigned char>(buffer[offset + i])) << (i * 8);
    }
    offset += 4;
    return true;
}

bool read_u64_le(const std::string &buffer, size_t &offset, uint64_t &value) {
    if (offset + 8 > buffer.size()) {
        return false;
    }
    value = 0;
    for (int i = 0; i < 8; ++i) {
        value |= static_cast<uint64_t>(static_cast<unsigned char>(buffer[offset + i])) << (i * 8);
    }
    offset += 8;
    return true;
}
