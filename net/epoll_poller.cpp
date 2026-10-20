#include "epoll_poller.h"

#include "channel.h"

#include <cerrno>
#include <cstdio>
#include <sys/epoll.h>
#include <unistd.h>

EPollPoller::EPollPoller() : epoll_fd_(epoll_create1(EPOLL_CLOEXEC)) {
    if (epoll_fd_ == -1) std::perror("epoll_create1");
}

EPollPoller::~EPollPoller() {
    if (epoll_fd_ != -1) close(epoll_fd_);
}

bool EPollPoller::updateChannel(Channel* channel) {
    if (!valid()) return false;
    auto it = channels_.find(channel->fd());
    if (it != channels_.end() && it->second.channel != channel) {
        std::fprintf(stderr, "epoll update: channel mismatch for fd %d\n",
                     channel->fd());
        return false;
    }
    const bool added = it == channels_.end();
    const std::uint64_t token = added ? next_token_++ : it->second.token; //防止EventLoop 逐个处理时，前一个回调可能关闭后面某个事件对应的连接；操作系统随后又可能把它的 fd 分配给新连接。
    epoll_event event{};
    event.events = channel->events();
    event.data.u64 = token;
    if (epoll_ctl(epoll_fd_, added ? EPOLL_CTL_ADD : EPOLL_CTL_MOD,
                  channel->fd(), &event) == -1) {
        std::perror(added ? "epoll_ctl(ADD)" : "epoll_ctl(MOD)");
        return false;
    }
    if (added) {
        channels_.emplace(channel->fd(), Registration{channel, token});
        token_fds_.emplace(token, channel->fd());
    }
    return true;
}

void EPollPoller::removeChannel(Channel* channel) {
    auto it = channels_.find(channel->fd());
    if (it == channels_.end() || it->second.channel != channel) return;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, channel->fd(), nullptr) == -1 &&
        errno != ENOENT && errno != EBADF) {
        std::perror("epoll_ctl(DEL)");
    }
    token_fds_.erase(it->second.token);
    channels_.erase(it);
}

bool EPollPoller::poll(std::vector<ActiveEvent>& active) {
    epoll_event events[64];
    const int ready = epoll_wait(epoll_fd_, events, 64, -1);
    active.clear();
    if (ready == -1) {
        if (errno != EINTR) std::perror("epoll_wait");
        return errno == EINTR;
    }
    active.reserve(ready);
    for (int i = 0; i < ready; ++i) {
        active.push_back({events[i].data.u64, events[i].events});
    }
    return true;
}

Channel* EPollPoller::findChannel(std::uint64_t token) const {
    auto fd = token_fds_.find(token);
    if (fd == token_fds_.end()) return nullptr;
    auto channel = channels_.find(fd->second);
    return channel == channels_.end() ? nullptr : channel->second.channel;
}
