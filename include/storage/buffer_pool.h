#ifndef FLEXQL_BUFFER_POOL_H
#define FLEXQL_BUFFER_POOL_H

#include <cstdint>
#include <string>
#include <memory>
#include <unordered_map>
#include <list>
#include <mutex>
#include <vector>
#include <unistd.h>

constexpr size_t PAGE_SIZE = 4096;
using PageId = uint32_t;
using SlotId = uint32_t;

struct RecordId {
    PageId page_id;
    SlotId slot_id;
};

struct Page {
    char data[PAGE_SIZE] = {0};
    int table_fd = -1;
    PageId id = 0;
    bool is_dirty = false;
    uint32_t pin_count = 0;
};

struct CacheKey {
    int fd;
    PageId pid;
    bool operator==(const CacheKey& o) const { return fd == o.fd && pid == o.pid; }
};

struct CacheKeyHash {
    std::size_t operator()(const CacheKey& k) const {
        return std::hash<int>()(k.fd) ^ (std::hash<PageId>()(k.pid) << 1);
    }
};

class BufferPool {
public:
    BufferPool(size_t capacity);
    ~BufferPool();
    
    Page* fetch_page(int table_fd, PageId page_id, bool is_transient = false);
    Page* new_page(int table_fd, PageId new_page_id);
    void unpin_page(int table_fd, PageId page_id, bool is_dirty);
    
    // Direct page replacement for local-page builder insert path
    void push_page_direct(int table_fd, PageId page_id, const char* data);
    
    void flush_all();
    void flush_table(int table_fd);

private:
    bool flush_page(Page* page);
    void evict_if_needed();

    size_t capacity_;
    std::mutex mutex_;
    std::unordered_map<CacheKey, std::list<std::unique_ptr<Page>>::iterator, CacheKeyHash> page_map_;
    std::list<std::unique_ptr<Page>> pages_;
};

#endif
