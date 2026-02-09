/**
 * @file server.hpp
 * @brief C++ Network Server for PostgreSQL wire protocol
 */

#ifndef SQL_ENGINE_NETWORK_SERVER_HPP
#define SQL_ENGINE_NETWORK_SERVER_HPP

#include <cstdint>
#include <cstring>
#include <string>
#include <functional>
#include <atomic>
#include <thread>
#include <iostream>
#include <memory>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>

namespace cloudsql {
namespace network {

/**
 * @brief Server statistics (C++ class)
 */
class ServerStats {
public:
    std::atomic<uint64_t> connections_accepted{0};
    std::atomic<uint64_t> connections_active{0};
    std::atomic<uint64_t> queries_executed{0};
    std::atomic<uint64_t> bytes_received{0};
    std::atomic<uint64_t> bytes_sent{0};
    std::atomic<uint64_t> uptime_seconds{0};
};

/**
 * @brief Server callbacks using std::function
 */
struct ServerCallbacks {
    std::function<int(class Server* server, int client_fd)> on_connect;
    std::function<int(class Server* server, int client_fd, const std::string& sql)> on_query;
    std::function<int(class Server* server, int client_fd)> on_disconnect;
};

/**
 * @brief Server status enumeration
 */
enum class ServerStatus {
    Stopped,
    Starting,
    Running,
    Stopping,
    Error
};

/**
 * @brief Network Server class
 */
class Server {
public:
    /**
     * @brief Constructor
     */
    explicit Server(uint16_t port) 
        : port_(port), listen_fd_(-1), status_(ServerStatus::Stopped) {}

    /**
     * @brief Destructor
     */
    ~Server() {
        stop();
        if (listen_fd_ >= 0) {
            close(listen_fd_);
        }
    }

    /**
     * @brief Create a new server instance
     */
    static std::unique_ptr<Server> create(uint16_t port) {
        return std::make_unique<Server>(port);
    }

    /**
     * @brief Set server callbacks
     */
    void set_callbacks(const ServerCallbacks& callbacks) {
        callbacks_ = callbacks;
    }

    /**
     * @brief Start the server
     */
    bool start() {
        if (running_.load()) {
            return false;
        }

        listen_fd_ = socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) {
            return false;
        }

        int opt = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

        struct sockaddr_in addr;
        memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port_);

        if (bind(listen_fd_, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
            close(listen_fd_);
            return false;
        }

        if (listen(listen_fd_, 10) < 0) {
            close(listen_fd_);
            return false;
        }

        status_ = ServerStatus::Running;
        running_ = true;
        accept_thread_ = std::thread(&Server::accept_connections, this);
        return true;
    }

    /**
     * @brief Stop the server
     */
    bool stop() {
        if (!running_.load()) {
            return true;
        }

        status_ = ServerStatus::Stopping;
        running_ = false;

        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }

        status_ = ServerStatus::Stopped;
        return true;
    }

    /**
     * @brief Wait for server to stop
     */
    void wait() {
        if (accept_thread_.joinable()) {
            accept_thread_.join();
        }
    }

    /**
     * @brief Get server statistics
     */
    const ServerStats& get_stats() const {
        return stats_;
    }

    /**
     * @brief Get server status
     */
    ServerStatus get_status() const {
        return status_;
    }

    /**
     * @brief Get server port
     */
    uint16_t get_port() const {
        return port_;
    }

    /**
     * @brief Check if server is running
     */
    bool is_running() const {
        return running_.load();
    }

    /**
     * @brief Get status string
     */
    std::string get_status_string() const {
        switch (status_) {
            case ServerStatus::Stopped: return "Stopped";
            case ServerStatus::Starting: return "Starting";
            case ServerStatus::Running: return "Running";
            case ServerStatus::Stopping: return "Stopping";
            case ServerStatus::Error: return "Error";
            default: return "Unknown";
        }
    }

private:
    void accept_connections() {
        while (running_.load()) {
            struct sockaddr_in client_addr;
            socklen_t client_len = sizeof(client_addr);

            int client_fd = accept(listen_fd_, (struct sockaddr*)&client_addr, &client_len);
            if (client_fd < 0) {
                continue;
            }

            stats_.connections_accepted.fetch_add(1);
            stats_.connections_active.fetch_add(1);

            std::thread([this, client_fd]() {
                handle_connection(client_fd);
                stats_.connections_active.fetch_sub(1);
            }).detach();
        }
    }

    void handle_connection(int client_fd) {
        char buffer[4096];
        std::string query;

        while (running_.load()) {
            ssize_t bytes_read = recv(client_fd, buffer, sizeof(buffer) - 1, 0);
            if (bytes_read <= 0) {
                break;
            }

            buffer[bytes_read] = '\0';
            stats_.bytes_received.fetch_add(bytes_read);
            query += buffer;

            if (query.find(';') != std::string::npos) {
                stats_.queries_executed.fetch_add(1);
                query.clear();
            }
        }

        close(client_fd);
    }

    uint16_t port_;
    int listen_fd_;
    std::atomic<bool> running_{false};
    std::atomic<ServerStatus> status_;
    ServerCallbacks callbacks_;
    ServerStats stats_;
    std::thread accept_thread_;
};

} // namespace network
} // namespace cloudsql

#endif // SQL_ENGINE_NETWORK_SERVER_HPP
