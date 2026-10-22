#include "heartbeat_manager.h"

#include "event_loop.h"
#include "tcp_connection.h"

#include <mutex>
#include <stdexcept>
#include <unordered_map>
#include <utility>

struct HeartbeatManager::Entry {
    ConnectionId id;
    std::weak_ptr<TcpConnection> connection;
    EventLoop* loop;
    EventLoop::Clock::time_point last_activity;
    TimerId timer;
};

struct HeartbeatManager::State {
    explicit State(Options config) : options(config) {}
    Options options;
    mutable std::mutex mutex;
    bool stopped = false;
    std::unordered_map<ConnectionId, std::shared_ptr<Entry>> entries;
};

HeartbeatManager::HeartbeatManager() : HeartbeatManager(Options{}) {}

HeartbeatManager::HeartbeatManager(Options options) {
    if (options.interval <= std::chrono::milliseconds::zero() ||
        options.timeout <= options.interval) {
        throw std::invalid_argument("heartbeat timeout must exceed its positive interval");
    }
    state_ = std::make_shared<State>(options);
}

HeartbeatManager::~HeartbeatManager() {
    stop();
}

bool HeartbeatManager::add(ConnectionId id,
                           const std::shared_ptr<TcpConnection>& connection) {
    if (id == 0 || !connection) return false;
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    if (state->stopped || state->entries.count(id)) return false;
    auto entry = std::make_shared<Entry>(Entry{
        id, connection, &connection->loop(), EventLoop::Clock::now(), {}});
    entry->timer = entry->loop->runEvery(state->options.interval,
        [weak = std::weak_ptr<State>(state), entry] { check(weak, entry); });
    if (!entry->timer) return false;
    state->entries.emplace(id, std::move(entry));
    return true;
}

void HeartbeatManager::touch(ConnectionId id) {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    auto it = state->entries.find(id);
    if (it != state->entries.end()) it->second->last_activity = EventLoop::Clock::now();
}

void HeartbeatManager::remove(ConnectionId id) {
    std::shared_ptr<Entry> entry;
    {
        const auto state = state_;
        std::lock_guard<std::mutex> lock(state->mutex);
        auto it = state->entries.find(id);
        if (it == state->entries.end()) return;
        entry = std::move(it->second);
        state->entries.erase(it);
    }
    entry->loop->cancel(entry->timer);
}

void HeartbeatManager::stop() {
    std::unordered_map<ConnectionId, std::shared_ptr<Entry>> entries;
    {
        const auto state = state_;
        std::lock_guard<std::mutex> lock(state->mutex);
        state->stopped = true;
        entries.swap(state->entries);
    }
    for (const auto& item : entries) item.second->loop->cancel(item.second->timer);
}

std::size_t HeartbeatManager::size() const {
    const auto state = state_;
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->entries.size();
}

void HeartbeatManager::check(const std::weak_ptr<State>& weak,
                             const std::shared_ptr<Entry>& entry) {
    const auto state = weak.lock();
    if (!state) return;
    std::shared_ptr<TcpConnection> connection;
    bool expired = false;
    bool probe = false;
    {
        std::lock_guard<std::mutex> lock(state->mutex);
        auto it = state->entries.find(entry->id);
        if (state->stopped || it == state->entries.end() || it->second != entry) return;
        connection = entry->connection.lock();
        const auto idle = EventLoop::Clock::now() - entry->last_activity;
        expired = idle >= state->options.timeout;
        probe = idle >= state->options.interval;
        if (!connection || expired) state->entries.erase(it);
    }
    if (!connection || expired) entry->loop->cancel(entry->timer);
    // Never call connection callbacks under the manager lock: close callbacks
    // remove the connection and its heartbeat registration.
    if (connection) {
        if (expired) connection->close("heartbeat timeout");
        else if (probe) connection->send("PING\n");
    }
}
