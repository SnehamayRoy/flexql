#ifndef FLEXQL_SQL_PARSER_H
#define FLEXQL_SQL_PARSER_H

#include "types.h"

#include <optional>
#include <string>
#include <vector>

ColumnType parse_column_type(const std::string &token);
std::string column_type_name(ColumnType type);
std::optional<Operand> parse_operand(const std::string &token);
std::optional<Condition> parse_condition(const std::string &text);
bool parse_create_table(const std::string &sql, std::string &table_name, std::vector<Column> &columns, std::string &error);
bool parse_insert(const std::string &sql, std::string &table_name, std::vector<Row> &rows, std::string &error);
bool parse_select(const std::string &sql, SelectQuery &query, std::string &error);

#endif
