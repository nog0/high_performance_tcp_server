#pragma once

#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

class EventLoop;

class EventLoopThread {
public:
    EventLoopThread() = default;
    ~EventLoopThread();

    EventLoopThread(const EventLoopThread&) = delete;
    EventLoopThread& operator=(const EventLoopThread&) = delete;

    EventLoop* startLoop();
    void stop();

private:
    std::thread thread_;
    std::shared_ptr<EventLoop> loop_;
    std::mutex mutex_;
    std::condition_variable ready_condition_;
    bool ready_ = false;
};
