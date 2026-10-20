#include "acceptor.h"

#include "channel.h"
#include "event_loop.h"

#include <arpa/inet.h>
#include <cerrno>
#include <cstdio>
#include <sys/socket.h>
#include <unistd.h>
#include <utility>

Acceptor::Acceptor(EventLoop& loop, std::uint16_t port)
    : loop_(loop), port_(port) {}

Acceptor::~Acceptor() {
    stop();
}

void Acceptor::setNewConnectionCallback(NewConnectionCallback callback) {
    new_connection_callback_ = std::move(callback);
}

bool Acceptor::start() {
    if (!loop_.valid() || listen_fd_ != -1) return false;
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ == -1) {
        std::perror("socket");
        return false;
    }
    int one = 1;
    if (setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == -1) {
        std::perror("setsockopt(SO_REUSEADDR)");
        stop();
        return false;
    }
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port_);
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) == -1) {
        std::perror("bind");
        stop();
        return false;
    }
    if (::listen(listen_fd_, SOMAXCONN) == -1) {
        std::perror("listen");
        stop();
        return false;
    }

    channel_ = std::make_unique<Channel>(loop_, listen_fd_);
    channel_->setReadCallback([this] { handleRead(); });
    channel_->setCloseCallback([this] { loop_.quit(); });
    if (!channel_->enableReading()) {
        stop();
        return false;
    }
    return true;
}

void Acceptor::stop() {
    if (channel_) {
        channel_->remove();
        channel_.reset();
    }
    if (listen_fd_ != -1) {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
}

void Acceptor::handleRead() {
    while (true) {
        sockaddr_in peer{};
        socklen_t peer_len = sizeof(peer);
        const int fd = accept4(listen_fd_, reinterpret_cast<sockaddr*>(&peer),
                               &peer_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd == -1) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) return;
            if (errno == EINTR || errno == ECONNABORTED || errno == EPROTO) continue;
            std::perror("accept4");
            return;
        }
        if (new_connection_callback_) {
            new_connection_callback_(fd, peer);
        } else {
            ::close(fd);
        }
    }
}
