#pragma once

#include "timer_id.h"

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <utility>

class Channel;
class EventLoop;

// Owned by an EventLoop; destroy only after that loop and its callers stop.
class TimerQueue {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;
    using Callback = std::function<void()>;

    explicit TimerQueue(EventLoop& loop);
    ~TimerQueue();

    TimerQueue(const TimerQueue&) = delete;
    TimerQueue& operator=(const TimerQueue&) = delete;

    bool valid() const { return valid_; }
    // Thread safe. Zero interval is one shot; a positive interval repeats.
    TimerId addTimer(Callback callback, TimePoint expiration,
                     Duration interval = Duration::zero());
    // Thread safe and idempotent. Cannot interrupt a callback already executing.
    void cancel(TimerId id);

private:
    struct Timer {
        std::uint64_t sequence;
        Callback callback;
        TimePoint expiration;
        Duration interval;
        std::atomic<bool> cancelled{false};

        Timer(std::uint64_t id, Callback cb, TimePoint when, Duration repeat)
            : sequence(id), callback(std::move(cb)), expiration(when), interval(repeat) {}
    };
    using Key = std::pair<TimePoint, std::uint64_t>;

    void addInLoop(const std::shared_ptr<Timer>& timer);
    void cancelInLoop(const std::shared_ptr<Timer>& timer);
    void handleRead();
    void resetTimerfd();
    void forget(const std::shared_ptr<Timer>& timer);

    EventLoop& loop_;
    int timer_fd_ = -1;
    std::unique_ptr<Channel> channel_;
    bool valid_ = false;
    // Only the loop thread accesses expiration ordering and timerfd.
    std::map<Key, std::shared_ptr<Timer>> timers_;
    // Publish cancellation immediately, including during an expired batch.
    std::mutex active_mutex_;
    std::unordered_map<std::uint64_t, std::shared_ptr<Timer>> active_timers_;
};
