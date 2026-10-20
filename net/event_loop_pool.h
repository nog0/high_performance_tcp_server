#pragma once

#include <cstddef>
#include <memory>
#include <vector>

class EventLoop;
class EventLoopThread;

class EventLoopPool {
public:
    explicit EventLoopPool(std::size_t thread_count);
    ~EventLoopPool();

    EventLoopPool(const EventLoopPool&) = delete;
    EventLoopPool& operator=(const EventLoopPool&) = delete;

    bool start();
    void stop();
    EventLoop* nextLoop();

private:
    std::size_t thread_count_;
    std::size_t next_ = 0;
    std::vector<std::unique_ptr<EventLoopThread>> threads_;
    std::vector<EventLoop*> loops_;
};
