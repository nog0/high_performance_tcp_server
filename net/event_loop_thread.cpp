#include "event_loop_thread.h"

#include "event_loop.h"

#include <cstdio>
#include <system_error>

EventLoopThread::~EventLoopThread() {
    stop();
}

EventLoop* EventLoopThread::startLoop() {
    if (thread_.joinable()) return loop_.get();
    try {
        thread_ = std::thread([this] {
            auto loop = std::make_shared<EventLoop>();
            {
                std::lock_guard<std::mutex> lock(mutex_);
                loop_ = loop;
                ready_ = true;
            }
            ready_condition_.notify_one();
            if (loop->valid()) loop->run();
        });
    } catch (const std::system_error& error) {
        std::fprintf(stderr, "worker thread: %s\n", error.what());
        return nullptr;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    ready_condition_.wait(lock, [this] { return ready_; });
    EventLoop* loop = loop_.get();
    if (!loop->valid()) {
        lock.unlock();
        stop();
        return nullptr;
    }
    return loop;
}

void EventLoopThread::stop() {
    if (loop_) loop_->quit();
    if (thread_.joinable()) thread_.join();
}
