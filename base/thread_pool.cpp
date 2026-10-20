#include "thread_pool.h"

#include <stdexcept>
#include <utility>

thread_pool::thread_pool(std::size_t thread_count, std::size_t queue_capacity)
    : tasks_(queue_capacity), thread_count_(thread_count) {
    if (thread_count == 0) {
        throw std::invalid_argument("thread_count must be positive");
    }

    workers_.reserve(thread_count);
}

void thread_pool::submit(std::function<void()> task) {
    if (stop_.load()) {
        throw std::runtime_error("cannot submit task to a stopped thread pool");
    }

    tasks_.put(std::move(task));
}

void thread_pool::start() {
    if (!workers_.empty() && workers_.front().joinable()) {
        return;
    }

    stop_.store(false);
    workers_.clear();
    workers_.reserve(thread_count_);

    for (std::size_t i = 0; i < thread_count_; ++i) {
        workers_.emplace_back(&thread_pool::worker_loop, this);
    }
}

void thread_pool::worker_loop() {
    while (true) {
        std::function<void()> task = tasks_.take();
        if (!task && stop_.load()) {
            return;
        }
        task();
    }
}

void thread_pool::stop() {
    bool was_stopped = stop_.exchange(true);
    if (was_stopped) {
        return;
    }

    for (std::size_t i = 0; i < workers_.size(); ++i) {
        tasks_.put({});
    }

    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

thread_pool::~thread_pool() {
    stop();
}
