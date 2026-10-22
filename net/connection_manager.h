#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

class TcpConnection;

// Owns connections across worker loops. Stop those loops before destroying or
// clearing the manager; asynchronous operations always run on the owning loop.
class ConnectionManager {
public:
    using ConnectionId = std::uint64_t;
    using ConnectionPtr = std::shared_ptr<TcpConnection>;

    ConnectionManager() = default;
    ~ConnectionManager();

    ConnectionManager(const ConnectionManager&) = delete;
    ConnectionManager& operator=(const ConnectionManager&) = delete;

    // Thread safe. Re-adding the same object returns its existing ID.
    ConnectionId add(ConnectionPtr connection);
    ConnectionPtr remove(ConnectionId id);
    ConnectionPtr find(ConnectionId id) const;
    std::vector<ConnectionPtr> snapshot() const;
    std::size_t size() const;

    // True means a task was queued, not that the peer received the bytes.
    bool send(ConnectionId id, std::string message) const;
    bool shutdown(ConnectionId id) const;
    bool close(ConnectionId id, std::string reason = "connection closed") const;
    void closeAll(std::string reason = "server stopping") const;
    void clear();

private:
    mutable std::mutex mutex_;
    ConnectionId next_id_ = 1;
    std::unordered_map<ConnectionId, ConnectionPtr> connections_;
    std::unordered_map<const TcpConnection*, ConnectionId> ids_;
};
