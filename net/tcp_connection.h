#pragma once

#include "buffer.h"
#include "channel.h"

#include <cstddef>
#include <functional>
#include <memory>
#include <utility>
#include <string>

class EventLoop;

class TcpConnection : public std::enable_shared_from_this<TcpConnection> {
public:
    using MessageCallback = std::function<void(const std::shared_ptr<TcpConnection>&,
                                               const std::string&)>;
    using CloseCallback = std::function<void(int, const char*)>;
    using ActivityCallback = std::function<void()>;

    TcpConnection(EventLoop& loop, int fd);
    ~TcpConnection();

    TcpConnection(const TcpConnection&) = delete;
    TcpConnection& operator=(const TcpConnection&) = delete;

    int fd() const { return fd_; }
    EventLoop& loop() const { return loop_; }
    bool start();
    void send(const std::string& message);
    void shutdown();
    void close(const char* reason);
    void setMessageCallback(MessageCallback callback) { message_callback_ = std::move(callback); }
    void setCloseCallback(CloseCallback callback) { close_callback_ = std::move(callback); }
    // Set before start(); invoked on the owning loop after each successful read.
    void setActivityCallback(ActivityCallback callback) { activity_callback_ = std::move(callback); }

private:
    void handleRead();
    void handleWrite();

    static constexpr std::size_t kMaxInputBytes = 64 * 1024;
    static constexpr std::size_t kMaxOutputBytes = 1024 * 1024;

    EventLoop& loop_;
    int fd_;
    Channel channel_;
    Buffer input_;
    Buffer output_;
    bool close_after_flush_ = false;
    bool closed_ = false;
    MessageCallback message_callback_;
    CloseCallback close_callback_;
    ActivityCallback activity_callback_;
};
