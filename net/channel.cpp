#include "channel.h"

#include "event_loop.h"

bool Channel::update(std::uint32_t events) {
    const auto previous = events_;
    events_ = events;
    if (loop_.updateChannel(this)) return true;
    events_ = previous;
    return false;
}

bool Channel::enableReading() { return update(events_ | EPOLLIN | EPOLLRDHUP | EPOLLET); }
bool Channel::enableWriting() { return update(events_ | EPOLLOUT); }
bool Channel::disableWriting() { return update(events_ & ~EPOLLOUT); }
void Channel::remove() { loop_.removeChannel(this); }

void Channel::handleEvent(std::uint32_t revents) {
    // Keep a TcpConnection alive if its callback removes it from the server.
    std::shared_ptr<void> guard;
    if (tied_) {
        guard = owner_.lock();
        if (!guard) return;
    }
    if ((revents & EPOLLIN) && read_callback_) read_callback_();
    if ((revents & EPOLLOUT) && write_callback_) write_callback_();
    if ((revents & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) && close_callback_) {
        close_callback_();
    }
}
