#pragma once

#include "epoll_poller.h"
#include "../timer/timer_id.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

class Channel;
class TimerQueue;

class EventLoop {
public:
    using Functor = std::function<void()>;
    using Clock = std::chrono::steady_clock;

    EventLoop();
    ~EventLoop();

    EventLoop(const EventLoop&) = delete;
    EventLoop& operator=(const EventLoop&) = delete;

    bool valid() const { return valid_; }
    void run();
    void quit();
    // Safe to call from another thread. The functor runs on this loop's thread.
    void queueInLoop(Functor functor);
    // Thread safe; callbacks execute on this loop. Repeating timers use fixed delay.
    TimerId runAt(Clock::time_point when, Functor callback);
    TimerId runAfter(Clock::duration delay, Functor callback);
    TimerId runEvery(Clock::duration interval, Functor callback);
    void cancel(TimerId id);
    bool updateChannel(Channel* channel) { return poller_.updateChannel(channel); }
    void removeChannel(Channel* channel) { poller_.removeChannel(channel); }

private:
    void wakeup();
    void handleWakeup();
    void runPendingFunctors();

    EPollPoller poller_;
    int wakeup_fd_ = -1;
    std::unique_ptr<Channel> wakeup_channel_;
    std::unique_ptr<TimerQueue> timer_queue_;
    std::mutex pending_mutex_;
    std::vector<Functor> pending_functors_;
    std::atomic<bool> quit_{false};
    bool valid_ = false;
};
