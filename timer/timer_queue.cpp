#include "timer_queue.h"

#include "../net/channel.h"
#include "../net/event_loop.h"

#include <cerrno>
#include <cstdio>
#include <stdexcept>
#include <sys/timerfd.h>
#include <unistd.h>
#include <vector>

namespace {
// IDs are unique across queues, so another loop cannot cancel an unrelated timer.
std::atomic<std::uint64_t> next_sequence{1};
}

TimerQueue::TimerQueue(EventLoop& loop) : loop_(loop) {
    timer_fd_ = ::timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK | TFD_CLOEXEC);
    if (timer_fd_ == -1) {
        std::perror("timerfd_create");
        return;
    }
    channel_ = std::make_unique<Channel>(loop_, timer_fd_);
    channel_->setReadCallback([this] { handleRead(); });
    valid_ = channel_->enableReading();
}

TimerQueue::~TimerQueue() {
    if (channel_) channel_->remove();
    if (timer_fd_ != -1) ::close(timer_fd_);
}

TimerId TimerQueue::addTimer(Callback callback, TimePoint expiration, Duration interval) {
    if (!callback || interval < Duration::zero()) {
        throw std::invalid_argument("timer needs a callback and a nonnegative interval");
    }
    if (!valid_) return {};
    const auto sequence = next_sequence.fetch_add(1);
    auto timer = std::make_shared<Timer>(sequence, std::move(callback), expiration, interval);
    {
        std::lock_guard<std::mutex> lock(active_mutex_);
        active_timers_.emplace(sequence, timer);
    }
    loop_.queueInLoop([this, timer] { addInLoop(timer); });
    return TimerId(sequence);
}

void TimerQueue::cancel(TimerId id) {
    std::shared_ptr<Timer> timer;
    {
        std::lock_guard<std::mutex> lock(active_mutex_);
        auto it = active_timers_.find(id.sequence_);
        if (it == active_timers_.end()) return;
        timer = it->second;
        timer->cancelled.store(true);
    }
    loop_.queueInLoop([this, timer] { cancelInLoop(timer); });
}

void TimerQueue::forget(const std::shared_ptr<Timer>& timer) {
    std::lock_guard<std::mutex> lock(active_mutex_);
    active_timers_.erase(timer->sequence);
}

void TimerQueue::addInLoop(const std::shared_ptr<Timer>& timer) {
    if (timer->cancelled.load()) {
        forget(timer);
        return;
    }
    timers_.emplace(Key{timer->expiration, timer->sequence}, timer);
    resetTimerfd();
}

void TimerQueue::cancelInLoop(const std::shared_ptr<Timer>& timer) {
    timers_.erase(Key{timer->expiration, timer->sequence});
    forget(timer);
    resetTimerfd();
}

void TimerQueue::handleRead() {
    std::uint64_t count;
    while (::read(timer_fd_, &count, sizeof(count)) == -1) {
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        std::perror("timerfd read");
        return;
    }

    const auto now = Clock::now();
    std::vector<std::shared_ptr<Timer>> expired;
    auto it = timers_.begin();
    while (it != timers_.end() && it->first.first <= now) {
        expired.push_back(it->second);
        it = timers_.erase(it);
    }
    for (const auto& timer : expired) {
        if (!timer->cancelled.load()) timer->callback();
        if (!timer->cancelled.load() && timer->interval > Duration::zero()) {
            // Fixed delay: skip missed ticks, then wait an interval after completion.
            timer->expiration = Clock::now() + timer->interval;
            timers_.emplace(Key{timer->expiration, timer->sequence}, timer);
        } else {
            forget(timer);
        }
    }
    resetTimerfd();
}

void TimerQueue::resetTimerfd() {
    itimerspec value{};
    if (!timers_.empty()) {
        auto delay = std::chrono::duration_cast<std::chrono::nanoseconds>(
            timers_.begin()->first.first - Clock::now());
        // An all-zero it_value disarms timerfd; overdue tasks need a nonzero value.
        if (delay.count() <= 0) delay = std::chrono::nanoseconds(1);
        const auto seconds = std::chrono::duration_cast<std::chrono::seconds>(delay);
        value.it_value.tv_sec = seconds.count();
        value.it_value.tv_nsec = (delay - seconds).count();
    }
    // Relative CLOCK_MONOTONIC time avoids assuming steady_clock's epoch.
    while (::timerfd_settime(timer_fd_, 0, &value, nullptr) == -1) {
        if (errno == EINTR) continue;
        std::perror("timerfd_settime");
        break;
    }
}
