#ifndef FLEXQL_SQL_UTILS_H
#define FLEXQL_SQL_UTILS_H

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "types.h"

std::string trim(const std::string &value);
std::string strip_trailing_semicolon(const std::string &sql);
std::string to_upper(std::string input);
bool iequals(const std::string &a, const std::string &b);
bool is_number(const std::string &value);
bool parse_number(const std::string &value, long double &out);
std::vector<std::string> split_csv(const std::string &input);
std::string unquote(const std::string &value);
bool evaluate_comparison(const std::string &left, const std::string &op, const std::string &right);
bool evaluate_comparison(const Row::Value &left, const std::string &op, const Row::Value &right);
std::string format_decimal(double value);
std::string value_to_string(const Row::Value &value);
bool coerce_value(ColumnType type, const Row::Value &input, Row::Value &output);
bool coerce_row(const std::vector<Column> &columns, Row &row);

void append_u32_le(std::string &out, uint32_t value);
void append_u64_le(std::string &out, uint64_t value);
bool read_u32_le(const std::string &buffer, size_t &offset, uint32_t &value);
bool read_u64_le(const std::string &buffer, size_t &offset, uint64_t &value);

#endif
