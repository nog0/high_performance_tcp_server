#include "event_loop.h"

#include "channel.h"
#include "../timer/timer_queue.h"

#include <cerrno>
#include <cstdio>
#include <cstdint>
#include <stdexcept>
#include <sys/eventfd.h>
#include <unistd.h>
#include <utility>

EventLoop::EventLoop() {
    if (!poller_.valid()) return;
    wakeup_fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC);
    if (wakeup_fd_ == -1) {
        std::perror("eventfd");
        return;
    }
    wakeup_channel_ = std::make_unique<Channel>(*this, wakeup_fd_);
    wakeup_channel_->setReadCallback([this] { handleWakeup(); });
    if (!wakeup_channel_->enableReading()) return;
    timer_queue_ = std::make_unique<TimerQueue>(*this);
    valid_ = timer_queue_->valid();
}

EventLoop::~EventLoop() {
    timer_queue_.reset();
    if (wakeup_channel_) wakeup_channel_->remove();
    if (wakeup_fd_ != -1) ::close(wakeup_fd_);
}

TimerId EventLoop::runAt(Clock::time_point when, Functor callback) {
    if (!valid_) return {};
    return timer_queue_->addTimer(std::move(callback), when);
}

TimerId EventLoop::runAfter(Clock::duration delay, Functor callback) {
    return runAt(Clock::now() + delay, std::move(callback));
}

TimerId EventLoop::runEvery(Clock::duration interval, Functor callback) {
    if (interval <= Clock::duration::zero()) {
        throw std::invalid_argument("repeating timer interval must be positive");
    }
    if (!valid_) return {};
    return timer_queue_->addTimer(std::move(callback), Clock::now() + interval, interval);
}

void EventLoop::cancel(TimerId id) {
    if (timer_queue_) timer_queue_->cancel(id);
}

void EventLoop::quit() {
    quit_.store(true);
    wakeup();
}

void EventLoop::queueInLoop(Functor functor) {
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending_functors_.push_back(std::move(functor));
    }
    wakeup();
}

void EventLoop::wakeup() {
    if (wakeup_fd_ == -1) return;
    const std::uint64_t one = 1;
    while (::write(wakeup_fd_, &one, sizeof(one)) == -1) {
        if (errno == EINTR) continue;
        if (errno != EAGAIN) std::perror("eventfd write");
        break;
    }
}

void EventLoop::handleWakeup() {
    std::uint64_t value;
    while (true) {
        const ssize_t count = ::read(wakeup_fd_, &value, sizeof(value));
        if (count == sizeof(value)) continue;
        if (count == -1 && errno == EINTR) continue;
        if (count == -1 && errno != EAGAIN && errno != EWOULDBLOCK) {
            std::perror("eventfd read");
        }
        break;
    }
}

void EventLoop::runPendingFunctors() {
    std::vector<Functor> pending;
    {
        std::lock_guard<std::mutex> lock(pending_mutex_);
        pending.swap(pending_functors_);
    }
    for (auto& functor : pending) functor();
}

void EventLoop::run() {
    if (!valid()) return;
    std::vector<EPollPoller::ActiveEvent> active;
    while (!quit_.load()) {
        if (!poller_.poll(active)) break;
        for (const auto& event : active) {
            // A previous callback may have removed this channel in the same batch.
            if (Channel* channel = poller_.findChannel(event.token)) {
                channel->handleEvent(event.events);
            }
        }
        runPendingFunctors();
    }
}
