#include "buffer_pool.h"
#include <iostream>
#include <cstring>

BufferPool::BufferPool(size_t capacity) : capacity_(capacity) {}

BufferPool::~BufferPool() {
    flush_all();
}

void BufferPool::evict_if_needed() {
    while (pages_.size() >= capacity_) {
        bool evicted = false;
        // Find least recently used unpinned page (from back of list)
        for (auto it = pages_.rbegin(); it != pages_.rend(); ++it) {
            if ((*it)->pin_count == 0) {
                if ((*it)->is_dirty) {
                    flush_page(it->get());
                }
                CacheKey key{(*it)->table_fd, (*it)->id};
                page_map_.erase(key);
                pages_.erase(std::next(it).base());
                evicted = true;
                break;
            }
        }
        if (!evicted) {
            // All pages are pinned, this is an out-of-memory for buffer pool scenario.
            // For this project, we'll just allow it to slightly exceed capacity rather than crash.
            break;
        }
    }
}

Page* BufferPool::fetch_page(int table_fd, PageId page_id, bool is_transient) {
    std::lock_guard<std::mutex> lock(mutex_);
    CacheKey key{table_fd, page_id};
    auto it = page_map_.find(key);
    
    if (it != page_map_.end()) {
        auto page_list_it = it->second;
        Page* page = page_list_it->get();
        page->pin_count++;
        // If not transient, promote to MRU (front of list)
        if (!is_transient) {
            pages_.splice(pages_.begin(), pages_, page_list_it);
        } else {
            // Keep at its current position or move to back (LRU end). For transient, moving to back is okay.
            // pages_.splice(pages_.end(), pages_, page_list_it);
        }
        return page;
    }
    
    evict_if_needed();
    
    auto new_page = std::make_unique<Page>();
    new_page->table_fd = table_fd;
    new_page->id = page_id;
    new_page->pin_count = 1;
    new_page->is_dirty = false;
    
    // Read from disk
    ssize_t offset = static_cast<ssize_t>(page_id) * PAGE_SIZE;
    ssize_t bytes_read = ::pread(table_fd, new_page->data, PAGE_SIZE, offset);
    if (bytes_read < 0) {
        // read error
        return nullptr;
    }
    
    if (!is_transient) {
        pages_.push_front(std::move(new_page));
        page_map_[key] = pages_.begin();
    } else {
        pages_.push_back(std::move(new_page));
        page_map_[key] = std::prev(pages_.end());
    }
    
    return pages_.front().get();
}

Page* BufferPool::new_page(int table_fd, PageId page_id) {
    std::lock_guard<std::mutex> lock(mutex_);
    CacheKey key{table_fd, page_id};
    
    if (page_map_.find(key) != page_map_.end()) {
        return nullptr; // Page already exists
    }
    
    evict_if_needed();
    
    auto page = std::make_unique<Page>();
    page->table_fd = table_fd;
    page->id = page_id;
    page->pin_count = 1;
    page->is_dirty = true;
    
    pages_.push_front(std::move(page));
    page_map_[key] = pages_.begin();
    
    return pages_.front().get();
}

void BufferPool::unpin_page(int table_fd, PageId page_id, bool is_dirty) {
    std::lock_guard<std::mutex> lock(mutex_);
    CacheKey key{table_fd, page_id};
    auto it = page_map_.find(key);
    if (it != page_map_.end()) {
        Page* page = it->second->get();
        if (page->pin_count > 0) {
            page->pin_count--;
        }
        if (is_dirty) {
            page->is_dirty = true;
        }
    }
}

void BufferPool::push_page_direct(int table_fd, PageId page_id, const char* data) {
    std::lock_guard<std::mutex> lock(mutex_);
    CacheKey key{table_fd, page_id};
    auto it = page_map_.find(key);
    if (it != page_map_.end()) {
        Page* page = it->second->get();
        memcpy(page->data, data, PAGE_SIZE);
        page->is_dirty = true;
    } else {
        evict_if_needed();
        auto page = std::make_unique<Page>();
        page->table_fd = table_fd;
        page->id = page_id;
        page->pin_count = 0;
        page->is_dirty = true;
        memcpy(page->data, data, PAGE_SIZE);
        pages_.push_back(std::move(page));
        page_map_[key] = std::prev(pages_.end());
    }
}

bool BufferPool::flush_page(Page* page) {
    ssize_t offset = static_cast<ssize_t>(page->id) * PAGE_SIZE;
    ssize_t bytes_written = ::pwrite(page->table_fd, page->data, PAGE_SIZE, offset);
    if (bytes_written != PAGE_SIZE) {
        return false;
    }
    page->is_dirty = false;
    return true;
}

void BufferPool::flush_all() {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& page_ptr : pages_) {
        if (page_ptr->is_dirty) {
            flush_page(page_ptr.get());
        }
    }
}

void BufferPool::flush_table(int table_fd) {
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& page_ptr : pages_) {
        if (page_ptr->table_fd == table_fd && page_ptr->is_dirty) {
            flush_page(page_ptr.get());
        }
    }
}
