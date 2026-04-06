#ifndef FLEXQL_QUERY_EXECUTOR_H
#define FLEXQL_QUERY_EXECUTOR_H

#include "storage_engine.h"

#include <string>

ExecResult execute_sql(Database &database, const std::string &sql);

#endif
