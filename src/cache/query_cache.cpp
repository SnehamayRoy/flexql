#include "query_cache.h"

QueryCache::QueryCache(size_t capacity) : capacity_(capacity) {}

std::optional<QueryResult> QueryCache::get(
    const std::string &sql,
    const std::function<std::optional<uint64_t>(const std::string&)> &version_lookup) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = items_.find(sql);
    if (it == items_.end()) {
        return std::nullopt;
    }

    const CachedResult &entry = it->second.first;
    for (size_t i = 0; i < entry.table_names.size(); ++i) {
        auto current_version = version_lookup(entry.table_names[i]);
        if (!current_version.has_value() || current_version.value() != entry.versions[i]) {
            order_.erase(it->second.second);
            items_.erase(it);
            return std::nullopt;
        }
    }

    order_.splice(order_.begin(), order_, it->second.second);
    return entry.result;
}

void QueryCache::put(const std::string &sql, CachedResult entry) {
    std::lock_guard<std::mutex> lock(mutex_);
    auto existing = items_.find(sql);
    if (existing != items_.end()) {
        order_.erase(existing->second.second);
        items_.erase(existing);
    }

    order_.push_front(sql);
    items_.emplace(sql, std::make_pair(std::move(entry), order_.begin()));

    if (items_.size() > capacity_) {
        const std::string &victim = order_.back();
        items_.erase(victim);
        order_.pop_back();
    }
}
