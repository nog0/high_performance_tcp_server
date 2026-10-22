#include "tcp_connection.h"

#include "event_loop.h"

#include <cerrno>
#include <cstdio>
#include <sys/socket.h>
#include <unistd.h>

TcpConnection::TcpConnection(EventLoop& loop, int fd)
    : loop_(loop), fd_(fd), channel_(loop, fd) {}

TcpConnection::~TcpConnection() {
    if (!closed_) {
        channel_.remove();
        ::close(fd_);
    }
}

bool TcpConnection::start() {
    channel_.tie(shared_from_this());
    channel_.setReadCallback([this] { handleRead(); });
    channel_.setWriteCallback([this] { handleWrite(); });
    channel_.setCloseCallback([this] { close("peer closed or socket error"); });
    return channel_.enableReading();
}

void TcpConnection::send(const std::string& message) {
    const auto guard = shared_from_this();
    if (closed_ || close_after_flush_) return;
    if (message.size() > kMaxOutputBytes - output_.readableBytes()) {
        close("output buffer limit reached");
        return;
    }
    output_.append(message);
    if (!channel_.enableWriting()) close("epoll update failed");
}

void TcpConnection::shutdown() {
    if (closed_) return;
    close_after_flush_ = true;
    if (output_.empty()) close("quit");
}

void TcpConnection::close(const char* reason) {
    const auto guard = shared_from_this();
    if (closed_) return;
    closed_ = true;
    channel_.remove();
    ::close(fd_);
    if (close_callback_) close_callback_(fd_, reason);
}

void TcpConnection::handleRead() {
    if (closed_) return;
    constexpr std::size_t kReadChunk = 4096;
    while (!closed_) {
        input_.ensureWritableBytes(kReadChunk);
        const ssize_t count = recv(fd_, input_.beginWrite(), kReadChunk, 0);
        if (count > 0) {
            input_.hasWritten(static_cast<std::size_t>(count));
            if (activity_callback_) activity_callback_();
            if (closed_) return;
            if (input_.readableBytes() > kMaxInputBytes) {
                close("input buffer limit reached");
                return;
            }
            const char* newline;
            while (!closed_ && (newline = input_.findEOL()) != nullptr) {
                const auto length = static_cast<std::size_t>(newline - input_.peek());
                std::string line = input_.retrieveAsString(length);
                input_.retrieve(1);  // Consume the newline separately.
                if (!line.empty() && line.back() == '\r') line.pop_back();
                if (message_callback_) message_callback_(shared_from_this(), line);
                if (close_after_flush_) return;
            }
            continue;  // Drain the socket because this channel uses EPOLLET.
        }
        if (count == 0) {
            close("client sent EOF");
            return;
        }
        if (errno == EINTR) continue;
        if (errno == EAGAIN || errno == EWOULDBLOCK) return;
        std::perror("recv");
        close("recv failed");
        return;
    }
}

void TcpConnection::handleWrite() {
    if (closed_) return;
    while (!output_.empty()) {
        const ssize_t count =
            ::send(fd_, output_.peek(), output_.readableBytes(), MSG_NOSIGNAL);
        if (count > 0) {
            output_.retrieve(static_cast<std::size_t>(count));
            continue;  // Drain pending bytes because this channel uses EPOLLET.
        }
        if (count == -1 && errno == EINTR) continue;
        if (count == -1 && (errno == EAGAIN || errno == EWOULDBLOCK)) return;
        std::perror("send");
        close("send failed");
        return;
    }
    if (close_after_flush_) {
        close("quit");
    } else if (!channel_.disableWriting()) {
        close("epoll update failed");
    }
}
