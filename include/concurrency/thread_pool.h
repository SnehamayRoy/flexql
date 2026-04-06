#ifndef FLEXQL_CONCURRENCY_THREAD_POOL_H
#define FLEXQL_CONCURRENCY_THREAD_POOL_H

#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <queue>
#include <thread>
#include <vector>

class ThreadPool {
public:
    ThreadPool() = default;
    ~ThreadPool();

    void start(size_t thread_count);
    void submit(std::function<void()> task);
    void stop();

private:
    std::vector<std::thread> workers_;
    std::queue<std::function<void()>> tasks_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool stopping_ = false;
};

#endif
