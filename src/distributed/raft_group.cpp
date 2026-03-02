/**
 * @file raft_group.cpp
 * @brief Raft consensus group implementation
 */

#include "distributed/raft_group.hpp"

#include <sys/socket.h>

#include <chrono>
#include <cstring>
#include <iostream>
#include <mutex>
#include <random>
#include <thread>
#include <vector>

namespace cloudsql::raft {

namespace {
constexpr int TIMEOUT_MIN_MS = 150;
constexpr int TIMEOUT_MAX_MS = 300;
constexpr int HEARTBEAT_INTERVAL_MS = 50;
constexpr int ELECTION_RETRY_MS = 100;
constexpr size_t VOTE_REPLY_SIZE = 9;
constexpr size_t APPEND_REPLY_SIZE = 9;
}  // namespace

RaftGroup::RaftGroup(uint16_t group_id, std::string node_id, cluster::ClusterManager& cluster_manager,
                     network::RpcServer& rpc_server)
    : group_id_(group_id),
      node_id_(std::move(node_id)),
      cluster_manager_(cluster_manager),
      rpc_server_(rpc_server),
      rng_(std::random_device{}()) {
    last_heartbeat_ = std::chrono::system_clock::now();
}

RaftGroup::~RaftGroup() {
    stop();
}

void RaftGroup::start() {
    running_ = true;
    raft_thread_ = std::thread(&RaftGroup::run_loop, this);
    // Note: RPC handlers are now managed by RaftManager
}

void RaftGroup::stop() {
    running_ = false;
    cv_.notify_all();
    if (raft_thread_.joinable()) {
        raft_thread_.join();
    }
}

void RaftGroup::run_loop() {
    while (running_) {
        switch (state_.load()) {
            case NodeState::Follower:
                do_follower();
                break;
            case NodeState::Candidate:
                do_candidate();
                break;
            case NodeState::Leader:
                do_leader();
                break;
            case NodeState::Shutdown:
                return;
        }
    }
}

void RaftGroup::do_follower() {
    const auto timeout = get_random_timeout();
    std::unique_lock<std::mutex> lock(mutex_);
    if (cv_.wait_for(lock, timeout, [this] {
            return !running_ ||
                   (std::chrono::system_clock::now() - last_heartbeat_ > get_random_timeout());
        })) {
        if (!running_) {
            return;
        }
        // Election timeout reached, become candidate
        state_ = NodeState::Candidate;
    }
}

void RaftGroup::do_candidate() {
    {
        const std::scoped_lock<std::mutex> lock(mutex_);
        persistent_state_.current_term++;
        persistent_state_.voted_for = node_id_;
        persist_state();
        last_heartbeat_ = std::chrono::system_clock::now();
    }

    auto peers = cluster_manager_.get_coordinators();
    size_t votes = 1;  // Vote for self
    const size_t needed = (peers.size() / 2) + 1;

    RequestVoteArgs args{};
    {
        const std::scoped_lock<std::mutex> lock(mutex_);
        args.term = persistent_state_.current_term;
        args.candidate_id = node_id_;
        args.last_log_index =
            persistent_state_.log.empty() ? 0 : persistent_state_.log.back().index;
        args.last_log_term = persistent_state_.log.empty() ? 0 : persistent_state_.log.back().term;
    }

    // Send RequestVote to peers
    for (const auto& peer : peers) {
        if (peer.id == node_id_) {
            continue;
        }

        network::RpcClient client(peer.address, peer.cluster_port);
        if (client.connect()) {
            std::vector<uint8_t> reply_payload;
            network::RpcHeader h;
            h.type = network::RpcType::RequestVote;
            h.group_id = group_id_;
            auto payload = args.serialize();
            h.payload_len = static_cast<uint16_t>(payload.size());

            if (client.call(h.type, payload, reply_payload)) {
                if (reply_payload.size() >= VOTE_REPLY_SIZE) {
                    term_t resp_term = 0;
                    std::memcpy(&resp_term, reply_payload.data(), 8);
                    const bool granted = reply_payload[8] != 0;

                    if (resp_term > args.term) {
                        step_down(resp_term);
                        return;
                    }
                    if (granted) {
                        votes++;
                    }
                }
            }
        }
    }

    if (votes >= needed) {
        state_ = NodeState::Leader;
        // Initialize leader state
        const std::scoped_lock<std::mutex> lock(mutex_);
        for (const auto& peer : peers) {
            leader_state_.next_index[peer.id] = persistent_state_.log.size() + 1;
            leader_state_.match_index[peer.id] = 0;
        }
    } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(ELECTION_RETRY_MS));
    }
}

void RaftGroup::do_leader() {
    auto peers = cluster_manager_.get_coordinators();
    for (const auto& peer : peers) {
        if (peer.id == node_id_) {
            continue;
        }
        // Send Heartbeat (AppendEntries with no entries)
        std::vector<uint8_t> args_payload(24, 0);  // Minimal heartbeat
        {
            const std::scoped_lock<std::mutex> lock(mutex_);
            const term_t t = persistent_state_.current_term;
            std::memcpy(args_payload.data(), &t, 8);
        }

        network::RpcClient client(peer.address, peer.cluster_port);
        if (client.connect()) {
            // Note: In a full multi-raft implementation, we'd need to set the group_id in header.
            // For now, RpcClient::send_only doesn't take group_id. We'll need to update it.
            static_cast<void>(client.send_only(network::RpcType::AppendEntries, args_payload));
        }
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(HEARTBEAT_INTERVAL_MS));
}

void RaftGroup::handle_request_vote(const network::RpcHeader& header,
                                    const std::vector<uint8_t>& payload, int client_fd) {
    (void)header;
    if (payload.size() < 24) {
        return;
    }

    term_t term = 0;
    uint64_t id_len = 0;
    std::memcpy(&term, payload.data(), 8);
    std::memcpy(&id_len, payload.data() + 8, 8);
    const std::string candidate_id(reinterpret_cast<const char*>(payload.data() + 16), id_len);

    std::scoped_lock<std::mutex> lock(mutex_);
    RequestVoteReply reply{};
    reply.term = persistent_state_.current_term;
    reply.vote_granted = false;

    if (term > persistent_state_.current_term) {
        step_down(term);
    }

    if (term == persistent_state_.current_term &&
        (persistent_state_.voted_for.empty() || persistent_state_.voted_for == candidate_id)) {
        persistent_state_.voted_for = candidate_id;
        persist_state();
        reply.vote_granted = true;
        last_heartbeat_ = std::chrono::system_clock::now();
    }

    std::vector<uint8_t> out(VOTE_REPLY_SIZE);
    std::memcpy(out.data(), &reply.term, 8);
    out[8] = reply.vote_granted ? 1 : 0;

    // Send response back
    network::RpcHeader resp_h;
    resp_h.type = network::RpcType::RequestVote;
    resp_h.group_id = group_id_;
    resp_h.payload_len = static_cast<uint16_t>(VOTE_REPLY_SIZE);
    char h_buf[RpcHeader::HEADER_SIZE];
    resp_h.encode(h_buf);
    static_cast<void>(send(client_fd, h_buf, RpcHeader::HEADER_SIZE, 0));
    static_cast<void>(send(client_fd, out.data(), out.size(), 0));
}

void RaftGroup::handle_append_entries(const network::RpcHeader& header,
                                      const std::vector<uint8_t>& payload, int client_fd) {
    (void)header;
    if (payload.size() < 8) {
        return;
    }

    term_t term = 0;
    std::memcpy(&term, payload.data(), 8);

    std::scoped_lock<std::mutex> lock(mutex_);
    AppendEntriesReply reply{};
    reply.term = persistent_state_.current_term;
    reply.success = false;

    if (term >= persistent_state_.current_term) {
        if (term > persistent_state_.current_term) {
            step_down(term);
        }
        state_ = NodeState::Follower;
        last_heartbeat_ = std::chrono::system_clock::now();
        reply.success = true;
    }

    std::vector<uint8_t> out(APPEND_REPLY_SIZE);
    std::memcpy(out.data(), &reply.term, 8);
    out[8] = reply.success ? 1 : 0;

    network::RpcHeader resp_h;
    resp_h.type = network::RpcType::AppendEntries;
    resp_h.group_id = group_id_;
    resp_h.payload_len = static_cast<uint16_t>(APPEND_REPLY_SIZE);
    char h_buf[RpcHeader::HEADER_SIZE];
    resp_h.encode(h_buf);
    static_cast<void>(send(fd, h_buf, RpcHeader::HEADER_SIZE, 0));
    static_cast<void>(send(client_fd, out.data(), out.size(), 0));
}

void RaftGroup::step_down(term_t new_term) {
    persistent_state_.current_term = new_term;
    persistent_state_.voted_for = "";
    state_ = NodeState::Follower;
    persist_state();
}

std::chrono::milliseconds RaftGroup::get_random_timeout() const {
    std::uniform_int_distribution<int> dist(TIMEOUT_MIN_MS, TIMEOUT_MAX_MS);
    auto& mutable_rng = const_cast<std::mt19937&>(rng_);
    return std::chrono::milliseconds(dist(mutable_rng));
}

void RaftGroup::persist_state() { /* TODO */ }
void RaftGroup::load_state() { /* TODO */ }

bool RaftGroup::replicate(const std::string& command) {
    if (state_.load() != NodeState::Leader) {
        return false;
    }
    (void)command;
    return true;
}

}  // namespace cloudsql::raft
