/**
 * @file raft_manager.hpp
 * @brief Manages multiple Raft consensus groups on a single node
 */

#ifndef SQL_ENGINE_DISTRIBUTED_RAFT_MANAGER_HPP
#define SQL_ENGINE_DISTRIBUTED_RAFT_MANAGER_HPP

#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "distributed/raft_group.hpp"
#include "network/rpc_server.hpp"

namespace cloudsql::raft {

/**
 * @brief Manager for Multi-Raft implementation
 */
class RaftManager {
   public:
    RaftManager(std::string node_id, cluster::ClusterManager& cluster_manager,
                network::RpcServer& rpc_server);
    ~RaftManager() = default;

    // Prevent copying
    RaftManager(const RaftManager&) = delete;
    RaftManager& operator=(const RaftManager&) = delete;

    void start();
    void stop();

    /**
     * @brief Create or get a Raft group
     */
    std::shared_ptr<RaftGroup> get_or_create_group(uint16_t group_id);

    /**
     * @brief Get an existing group
     */
    std::shared_ptr<RaftGroup> get_group(uint16_t group_id);

   private:
    /**
     * @brief Route incoming Raft RPCs to the correct group
     */
    void handle_raft_rpc(const network::RpcHeader& header, const std::vector<uint8_t>& payload,
                         int client_fd);

    std::string node_id_;
    cluster::ClusterManager& cluster_manager_;
    network::RpcServer& rpc_server_;

    std::mutex mutex_;
    std::unordered_map<uint16_t, std::shared_ptr<RaftGroup>> groups_;
};

}  // namespace cloudsql::raft

#endif  // SQL_ENGINE_DISTRIBUTED_RAFT_MANAGER_HPP
