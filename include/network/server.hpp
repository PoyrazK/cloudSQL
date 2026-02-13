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

#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"
#include "executor/query_executor.hpp"

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
    Server(uint16_t port, Catalog& catalog, storage::StorageManager& storage_manager);

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
    static std::unique_ptr<Server> create(uint16_t port, Catalog& catalog, storage::StorageManager& storage_manager);

    /**
     * @brief Start the server
     */
    bool start();

    /**
     * @brief Stop the server
     */
    bool stop();

    /**
     * @brief Wait for server to stop
     */
    void wait();

    const ServerStats& get_stats() const { return stats_; }
    ServerStatus get_status() const { return status_; }
    uint16_t get_port() const { return port_; }
    bool is_running() const { return running_.load(); }
    std::string get_status_string() const;

private:
    void accept_connections();
    void handle_connection(int client_fd);

    uint16_t port_;
    int listen_fd_;
    std::atomic<bool> running_{false};
    std::atomic<ServerStatus> status_;
    
    Catalog& catalog_;
    storage::StorageManager& storage_manager_;
    executor::QueryExecutor executor_;
    
    ServerStats stats_;
    std::thread accept_thread_;
};

} // namespace network
} // namespace cloudsql

#endif // SQL_ENGINE_NETWORK_SERVER_HPP
