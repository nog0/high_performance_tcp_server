#include "event_loop_pool.h"

#include "event_loop_thread.h"

EventLoopPool::EventLoopPool(std::size_t thread_count)
    : thread_count_(thread_count) {}

EventLoopPool::~EventLoopPool() {
    stop();
}

bool EventLoopPool::start() {
    if (!threads_.empty() || thread_count_ == 0) return false;
    threads_.reserve(thread_count_);
    loops_.reserve(thread_count_);
    for (std::size_t i = 0; i < thread_count_; ++i) {
        auto thread = std::make_unique<EventLoopThread>();
        EventLoop* loop = thread->startLoop();
        if (!loop) {
            stop();
            return false;
        }
        loops_.push_back(loop);
        threads_.push_back(std::move(thread));
    }
    return true;
}

void EventLoopPool::stop() {
    for (auto& thread : threads_) thread->stop();
    loops_.clear();
    next_ = 0;
    // Keep the stopped EventLoops alive until the pool is destroyed. A server
    // can still be releasing TcpConnections that refer to those loops.
}

EventLoop* EventLoopPool::nextLoop() {
    if (loops_.empty()) return nullptr;
    EventLoop* loop = loops_[next_];
    next_ = (next_ + 1) % loops_.size();
    return loop;
}
