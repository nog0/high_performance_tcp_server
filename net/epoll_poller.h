#pragma once

#include <cstdint>
#include <unordered_map>
#include <vector>

class Channel;

class EPollPoller {
public:
    struct ActiveEvent {
        std::uint64_t token;
        std::uint32_t events;
    };

    EPollPoller();
    ~EPollPoller();

    EPollPoller(const EPollPoller&) = delete;
    EPollPoller& operator=(const EPollPoller&) = delete;

    bool valid() const { return epoll_fd_ != -1; }
    bool updateChannel(Channel* channel);
    void removeChannel(Channel* channel);
    bool poll(std::vector<ActiveEvent>& active);
    Channel* findChannel(std::uint64_t token) const;

private:
    struct Registration {
        Channel* channel;
        std::uint64_t token;
    };

    int epoll_fd_ = -1;
    std::uint64_t next_token_ = 1;
    std::unordered_map<int, Registration> channels_;
    std::unordered_map<std::uint64_t, int> token_fds_;
};
