#ifndef FLEXQL_QUERY_CACHE_H
#define FLEXQL_QUERY_CACHE_H

#include "types.h"

#include <functional>
#include <list>
#include <mutex>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

struct CachedResult {
    QueryResult result;
    std::vector<std::string> table_names;
    std::vector<uint64_t> versions;
};

class QueryCache {
public:
    explicit QueryCache(size_t capacity = 128);

    std::optional<QueryResult> get(
        const std::string &sql,
        const std::function<std::optional<uint64_t>(const std::string&)> &version_lookup);

    void put(const std::string &sql, CachedResult entry);

private:
    size_t capacity_;
    std::list<std::string> order_;
    std::unordered_map<std::string, std::pair<CachedResult, std::list<std::string>::iterator>> items_;
    std::mutex mutex_;
};

#endif
