#include "connection_manager.h"

#include "event_loop.h"
#include "tcp_connection.h"

#include <stdexcept>
#include <utility>

ConnectionManager::~ConnectionManager() {
    clear();
}

ConnectionManager::ConnectionId ConnectionManager::add(ConnectionPtr connection) {
    if (!connection) throw std::invalid_argument("cannot manage a null connection");
    std::lock_guard<std::mutex> lock(mutex_);
    auto existing = ids_.find(connection.get());
    if (existing != ids_.end()) return existing->second;
    const ConnectionId id = next_id_++;
    connections_.emplace(id, connection);
    ids_.emplace(connection.get(), id);
    return id;
}

ConnectionManager::ConnectionPtr ConnectionManager::remove(ConnectionId id) {
    ConnectionPtr connection;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        auto it = connections_.find(id);
        if (it == connections_.end()) return {};
        connection = std::move(it->second);
        ids_.erase(connection.get());
        connections_.erase(it);
    }
    return connection;
}

ConnectionManager::ConnectionPtr ConnectionManager::find(ConnectionId id) const {
    std::lock_guard<std::mutex> lock(mutex_);
    auto it = connections_.find(id);
    return it == connections_.end() ? ConnectionPtr{} : it->second;
}

std::vector<ConnectionManager::ConnectionPtr> ConnectionManager::snapshot() const {
    std::lock_guard<std::mutex> lock(mutex_);
    std::vector<ConnectionPtr> result;
    result.reserve(connections_.size());
    for (const auto& entry : connections_) result.push_back(entry.second);
    return result;
}

std::size_t ConnectionManager::size() const {
    std::lock_guard<std::mutex> lock(mutex_);
    return connections_.size();
}

bool ConnectionManager::send(ConnectionId id, std::string message) const {
    auto connection = find(id);
    if (!connection) return false;
    connection->loop().queueInLoop(
        [connection, message = std::move(message)] { connection->send(message); });
    return true;
}

bool ConnectionManager::shutdown(ConnectionId id) const {
    auto connection = find(id);
    if (!connection) return false;
    connection->loop().queueInLoop([connection] { connection->shutdown(); });
    return true;
}

bool ConnectionManager::close(ConnectionId id, std::string reason) const {
    auto connection = find(id);
    if (!connection) return false;
    connection->loop().queueInLoop(
        [connection, reason = std::move(reason)] { connection->close(reason.c_str()); });
    return true;
}

void ConnectionManager::closeAll(std::string reason) const {
    for (const auto& connection : snapshot()) {
        connection->loop().queueInLoop(
            [connection, reason] { connection->close(reason.c_str()); });
    }
}

void ConnectionManager::clear() {
    std::unordered_map<ConnectionId, ConnectionPtr> connections;
    {
        std::lock_guard<std::mutex> lock(mutex_);
        connections.swap(connections_);
        ids_.clear();
    }
    // Release ownership outside the registry lock.
}
