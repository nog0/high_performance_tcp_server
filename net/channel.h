#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <utility>
#include <sys/epoll.h>

class EventLoop;

class Channel {
public:
    Channel(EventLoop& loop, int fd) : loop_(loop), fd_(fd) {}

    Channel(const Channel&) = delete;
    Channel& operator=(const Channel&) = delete;

    int fd() const { return fd_; }
    std::uint32_t events() const { return events_; }
    void setReadCallback(std::function<void()> callback) { read_callback_ = std::move(callback); }
    void setWriteCallback(std::function<void()> callback) { write_callback_ = std::move(callback); }
    void setCloseCallback(std::function<void()> callback) { close_callback_ = std::move(callback); }
    void tie(const std::shared_ptr<void>& owner) { owner_ = owner; tied_ = true; }

    bool enableReading();
    bool enableWriting();
    bool disableWriting();
    void remove();
    void handleEvent(std::uint32_t revents);

private:
    bool update(std::uint32_t events);

    EventLoop& loop_;
    int fd_;
    std::uint32_t events_ = 0;
    std::weak_ptr<void> owner_;
    bool tied_ = false;
    std::function<void()> read_callback_;
    std::function<void()> write_callback_;
    std::function<void()> close_callback_;
};
