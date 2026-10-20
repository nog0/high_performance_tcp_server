#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <netinet/in.h>

class Channel;
class EventLoop;

class Acceptor {
public:
    using NewConnectionCallback = std::function<void(int, const sockaddr_in&)>;

    Acceptor(EventLoop& loop, std::uint16_t port);
    ~Acceptor();

    Acceptor(const Acceptor&) = delete;
    Acceptor& operator=(const Acceptor&) = delete;

    void setNewConnectionCallback(NewConnectionCallback callback);
    bool start();
    void stop();

private:
    void handleRead();

    EventLoop& loop_;
    std::uint16_t port_;
    int listen_fd_ = -1;
    std::unique_ptr<Channel> channel_;
    NewConnectionCallback new_connection_callback_;
};
