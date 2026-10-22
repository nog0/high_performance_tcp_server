#include "../net/acceptor.h"
#include "../net/connection_manager.h"
#include "../net/event_loop.h"
#include "../net/event_loop_pool.h"
#include "../net/heartbeat_manager.h"
#include "../net/tcp_connection.h"

#include <arpa/inet.h>
#include <cctype>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstdint>
#include <iostream>
#include <memory>
#include <mutex>
#include <signal.h>
#include <string>
#include <unordered_map>
#include <utility>
#include <unistd.h>

namespace {

bool valid_name(const std::string& name) {
    if (name.empty() || name.size() > 24) return false;
    for (unsigned char c : name) {
        if (!std::isalnum(c) && c != '_' && c != '-') return false;
    }
    return true;
}

std::string after_first_word(const std::string& text, std::size_t start) {
    while (start < text.size() && text[start] == ' ') ++start;
    return text.substr(start);
}

class ChatServer {
public:
    ChatServer(std::uint16_t port, std::size_t worker_count,
               HeartbeatManager::Options heartbeat_options)
        : port_(port), workers_(worker_count), heartbeats_(heartbeat_options),
          acceptor_(main_loop_, port) {
        acceptor_.setNewConnectionCallback(
            [this](int fd, const sockaddr_in& peer) { accept_client(fd, peer); });
    }

    ~ChatServer() {
        acceptor_.stop();
        heartbeats_.stop();
        workers_.stop();
        connections_.clear();
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_.clear();
        names_.clear();
    }

    bool init() {
        if (!main_loop_.valid() || !workers_.start()) return false;
        return acceptor_.start();
    }

    void run() {
        std::cout << "chat server listening on 0.0.0.0:" << port_ << '\n';
        main_loop_.run();
    }

private:
    using ConnectionId = ConnectionManager::ConnectionId;

    struct Client {
        std::string name;
        bool quitting = false;
    };

    void accept_client(int fd, const sockaddr_in& peer) {
        EventLoop* worker = workers_.nextLoop();
        if (!worker) {
            ::close(fd);
            return;
        }
        auto connection = std::make_shared<TcpConnection>(*worker, fd);
        const ConnectionId id = connections_.add(connection);
        connection->setActivityCallback([this, id] { heartbeats_.touch(id); });
        connection->setMessageCallback(
            [this, id](const auto&, const auto& line) {
                handle_command(id, line);
            });
        connection->setCloseCallback(
            [this, id](int closed_fd, const char* reason) {
                disconnect(id, closed_fd, reason);
            });

        char ip[INET_ADDRSTRLEN]{};
        inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            clients_.emplace(id, Client{"", false});
            std::cout << "accepted " << ip << ':' << ntohs(peer.sin_port)
                      << " (fd=" << fd << ")\n";
        }
        worker->queueInLoop([this, id, connection] {
            if (!connection->start()) {
                connection->close("epoll registration failed");
            } else if (!heartbeats_.add(id, connection)) {
                connection->close("heartbeat registration failed");
            } else {
                connection->send("Welcome. Use /login <name> first.\n");
            }
        });
    }

    void handle_command(ConnectionId id, const std::string& line) {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        auto sender_it = clients_.find(id);
        if (sender_it == clients_.end() || sender_it->second.quitting) return;
        Client& sender = sender_it->second;

        if (line == "PONG") return;
        if (line == "PING") {
            connections_.send(id, "PONG\n");
            return;
        }

        if (line.rfind("/login ", 0) == 0) {
            const std::string name = after_first_word(line, 6);
            if (!valid_name(name)) {
                connections_.send(id, "ERROR: name must be 1-24 letters, digits, _, or -.\n");
            } else if (!sender.name.empty()) {
                connections_.send(id, "ERROR: already logged in as " + sender.name + ".\n");
            } else if (names_.count(name)) {
                connections_.send(id, "ERROR: name is already in use.\n");
            } else {
                sender.name = name;
                names_[name] = id;
                connections_.send(id, "OK: logged in as " + name + ".\n");
            }
            return;
        }
        if (line == "/quit") {
            sender.quitting = true;
            connections_.send(id, "bye\n");
            connections_.shutdown(id);
            return;
        }
        if (sender.name.empty()) {
            connections_.send(id, "ERROR: use /login <name> first.\n");
            return;
        }

        const std::string sender_name = sender.name;
        if (line.rfind("/broadcast ", 0) == 0) {
            const std::string message = after_first_word(line, 10);
            if (message.empty()) {
                connections_.send(id, "ERROR: /broadcast needs text.\n");
                return;
            }
            const std::string rendered = "[" + sender_name + "] " + message + "\n";
            for (const auto& entry : clients_) {
                if (!entry.second.name.empty() && !entry.second.quitting) {
                    connections_.send(entry.first, rendered);
                }
            }
            return;
        }
        if (line.rfind("/msg ", 0) == 0) {
            const std::string rest = after_first_word(line, 4);
            const std::size_t space = rest.find(' ');
            const std::string recipient_name = rest.substr(0, space);
            const std::string message =
                space == std::string::npos ? "" : after_first_word(rest, space);
            auto recipient_it = names_.find(recipient_name);
            if (recipient_name.empty() || message.empty()) {
                connections_.send(id, "ERROR: usage: /msg <name> <text>\n");
            } else if (recipient_it == names_.end()) {
                connections_.send(id, "ERROR: user " + recipient_name + " is not online.\n");
            } else {
                const ConnectionId recipient_id = recipient_it->second;
                auto target = clients_.find(recipient_id);
                if (target == clients_.end() || target->second.quitting) {
                    connections_.send(id, "ERROR: user " + recipient_name + " is not online.\n");
                    return;
                }
                connections_.send(recipient_id,
                                  "[private from " + sender_name + "] " + message + "\n");
                if (recipient_id != id) {
                    connections_.send(id,
                                      "[private to " + recipient_name + "] " + message + "\n");
                }
            }
            return;
        }
        connections_.send(id, "ERROR: commands: /login, /msg, /broadcast, /quit, PING, PONG\n");
    }

    void disconnect(ConnectionId id, int fd, const char* reason) {
        heartbeats_.remove(id);
        std::lock_guard<std::mutex> lock(clients_mutex_);
        connections_.remove(id);
        auto it = clients_.find(id);
        if (it == clients_.end()) return;
        if (!it->second.name.empty()) names_.erase(it->second.name);
        std::cout << "closed fd=" << fd << " (" << reason << ")\n";
        clients_.erase(it);
    }

    std::uint16_t port_;
    EventLoop main_loop_;
    EventLoopPool workers_;
    ConnectionManager connections_;
    HeartbeatManager heartbeats_;
    Acceptor acceptor_;
    std::mutex clients_mutex_;
    std::unordered_map<ConnectionId, Client> clients_;
    std::unordered_map<std::string, ConnectionId> names_;
};

bool parse_port(const char* text, std::uint16_t& result) {
    char* end = nullptr;
    const long value = std::strtol(text, &end, 10);
    if (*text == '\0' || *end != '\0' || value < 1 || value > 65535) return false;
    result = static_cast<std::uint16_t>(value);
    return true;
}

bool parse_workers(const char* text, std::size_t& result) {
    char* end = nullptr;
    errno = 0;
    const unsigned long value = std::strtoul(text, &end, 10);
    if (*text == '\0' || *end != '\0' || errno != 0 || value < 1 || value > 64) {
        return false;
    }
    result = static_cast<std::size_t>(value);
    return true;
}

bool parse_seconds(const char* text, std::chrono::milliseconds& result) {
    char* end = nullptr;
    errno = 0;
    const unsigned long value = std::strtoul(text, &end, 10);
    if (*text == '\0' || *end != '\0' || errno != 0 || value < 1 || value > 86400) {
        return false;
    }
    result = std::chrono::seconds(value);
    return true;
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 2 || argc > 5) {
        std::cerr << "Usage: " << argv[0]
                  << " <listen-port> [worker-count] [heartbeat-interval-seconds]"
                     " [heartbeat-timeout-seconds]\n";
        return 2;
    }
    std::uint16_t port;
    if (!parse_port(argv[1], port)) {
        std::cerr << "listen-port must be in 1..65535\n";
        return 2;
    }
    std::size_t worker_count = 4;
    if (argc >= 3 && !parse_workers(argv[2], worker_count)) {
        std::cerr << "worker-count must be in 1..64\n";
        return 2;
    }
    HeartbeatManager::Options heartbeat_options;
    if ((argc >= 4 && !parse_seconds(argv[3], heartbeat_options.interval)) ||
        (argc >= 5 && !parse_seconds(argv[4], heartbeat_options.timeout)) ||
        heartbeat_options.timeout <= heartbeat_options.interval) {
        std::cerr << "heartbeat seconds must be in 1..86400 and timeout must exceed interval\n";
        return 2;
    }
    signal(SIGPIPE, SIG_IGN);
    ChatServer server(port, worker_count, heartbeat_options);
    if (!server.init()) return 1;
    server.run();
}
