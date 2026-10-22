#pragma once

#include "connection_manager.h"

#include <chrono>
#include <cstddef>
#include <memory>

// Timer-based liveness checks. The connection's loop performs probes and closes.
// Stop the manager before destroying worker loops. Methods are thread safe.
class HeartbeatManager {
public:
    using ConnectionId = ConnectionManager::ConnectionId;
    struct Options {
        std::chrono::milliseconds interval{30000};
        std::chrono::milliseconds timeout{90000};
    };

    HeartbeatManager();
    explicit HeartbeatManager(Options options);
    ~HeartbeatManager();

    HeartbeatManager(const HeartbeatManager&) = delete;
    HeartbeatManager& operator=(const HeartbeatManager&) = delete;

    bool add(ConnectionId id, const std::shared_ptr<TcpConnection>& connection);
    void touch(ConnectionId id);
    void remove(ConnectionId id);
    void stop();
    std::size_t size() const;

private:
    struct Entry;
    struct State;
    static void check(const std::weak_ptr<State>& state,
                      const std::shared_ptr<Entry>& entry);
    std::shared_ptr<State> state_;
};
