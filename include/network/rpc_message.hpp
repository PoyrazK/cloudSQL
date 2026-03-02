/**
 * @file rpc_message.hpp
 * @brief Binary message format for internal cluster communication
 */

#ifndef SQL_ENGINE_NETWORK_RPC_MESSAGE_HPP
#define SQL_ENGINE_NETWORK_RPC_MESSAGE_HPP

#include <arpa/inet.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

#include "common/value.hpp"
#include "executor/types.hpp"

namespace cloudsql::network {

/**
 * @brief Internal RPC Message Types
 */
enum class RpcType : uint8_t {
    Heartbeat = 0,
    RegisterNode = 1,
    RequestVote = 2,
    AppendEntries = 3,
    ExecuteFragment = 4,
    QueryResults = 5,
    TxnPrepare = 6,
    TxnCommit = 7,
    TxnAbort = 8,
    PushData = 9,
    Error = 255
};

/**
 * @brief Serialization utilities for Common types
 */
class Serializer {
   public:
    static constexpr size_t VAL_SIZE_64 = 8;
    static constexpr size_t VAL_SIZE_32 = 4;

    static void serialize_value(const common::Value& val, std::vector<uint8_t>& out) {
        auto type = static_cast<uint8_t>(val.type());
        out.push_back(type);
        if (val.is_null()) {
            return;
        }

        switch (val.type()) {
            case common::ValueType::TYPE_INT64: {
                int64_t v = val.as_int64();
                const size_t offset = out.size();
                out.resize(offset + VAL_SIZE_64);
                std::memcpy(out.data() + offset, &v, VAL_SIZE_64);
                break;
            }
            case common::ValueType::TYPE_TEXT: {
                const std::string& s = val.as_text();
                const auto len = static_cast<uint32_t>(s.size());
                const size_t offset = out.size();
                out.resize(offset + VAL_SIZE_32 + len);
                std::memcpy(out.data() + offset, &len, VAL_SIZE_32);
                std::memcpy(out.data() + offset + VAL_SIZE_32, s.data(), len);
                break;
            }
            default:
                break;
        }
    }

    static common::Value deserialize_value(const uint8_t* data, size_t& offset, size_t size) {
        if (offset >= size) {
            return common::Value::make_null();
        }
        auto type = static_cast<common::ValueType>(data[offset++]);
        if (type == common::ValueType::TYPE_NULL) {
            return common::Value::make_null();
        }

        switch (type) {
            case common::ValueType::TYPE_INT64: {
                int64_t v = 0;
                if (offset + VAL_SIZE_64 <= size) {
                    std::memcpy(&v, data + offset, VAL_SIZE_64);
                    offset += VAL_SIZE_64;
                }
                return common::Value::make_int64(v);
            }
            case common::ValueType::TYPE_TEXT: {
                uint32_t len = 0;
                if (offset + VAL_SIZE_32 <= size) {
                    std::memcpy(&len, data + offset, VAL_SIZE_32);
                    offset += VAL_SIZE_32;
                }
                std::string s;
                if (offset + len <= size) {
                    s = std::string(reinterpret_cast<const char*>(data + offset), len);
                    offset += len;
                }
                return common::Value::make_text(s);
            }
            default:
                return common::Value::make_null();
        }
    }

    static void serialize_tuple(const executor::Tuple& tuple, std::vector<uint8_t>& out) {
        const auto count = static_cast<uint32_t>(tuple.size());
        const size_t offset = out.size();
        out.resize(offset + VAL_SIZE_32);
        std::memcpy(out.data() + offset, &count, VAL_SIZE_32);
        for (size_t i = 0; i < count; ++i) {
            serialize_value(tuple.get(i), out);
        }
    }

    static executor::Tuple deserialize_tuple(const uint8_t* data, size_t& offset, size_t size) {
        uint32_t count = 0;
        if (offset + VAL_SIZE_32 <= size) {
            std::memcpy(&count, data + offset, VAL_SIZE_32);
            offset += VAL_SIZE_32;
        }
        std::vector<common::Value> values;
        values.reserve(count);
        for (uint32_t i = 0; i < count; ++i) {
            values.push_back(deserialize_value(data, offset, size));
        }
        return executor::Tuple(std::move(values));
    }
};

/**
 * @brief Header for all internal RPC messages (fixed 8 bytes)
 */
struct RpcHeader {
    static constexpr uint32_t MAGIC = 0x4353514C;  // 'CSQL'
    static constexpr size_t HEADER_SIZE = 8;

    uint32_t magic = MAGIC;
    RpcType type = RpcType::Error;
    uint8_t flags = 0;
    uint16_t payload_len = 0;

    void encode(char* out) const {
        uint32_t n_magic = htonl(magic);
        uint16_t n_len = htons(payload_len);
        std::memcpy(out, &n_magic, 4);
        out[4] = static_cast<char>(type);
        out[5] = static_cast<char>(flags);
        std::memcpy(out + 6, &n_len, 2);
    }

    static RpcHeader decode(const char* in) {
        RpcHeader h;
        uint32_t n_magic = 0;
        uint16_t n_len = 0;
        std::memcpy(&n_magic, in, 4);
        h.magic = ntohl(n_magic);
        h.type = static_cast<RpcType>(static_cast<uint8_t>(in[4]));
        h.flags = static_cast<uint8_t>(in[5]);
        std::memcpy(&n_len, in + 6, 2);
        h.payload_len = ntohs(n_len);
        return h;
    }
};

/**
 * @brief Payload for executing a SQL fragment on a data node
 */
struct ExecuteFragmentArgs {
    std::string sql;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out(sql.size());
        std::memcpy(out.data(), sql.data(), sql.size());
        return out;
    }

    static ExecuteFragmentArgs deserialize(const std::vector<uint8_t>& in) {
        ExecuteFragmentArgs args;
        args.sql = std::string(reinterpret_cast<const char*>(in.data()), in.size());
        return args;
    }
};

/**
 * @brief Simple payload for returning query success/failure and data
 */
struct QueryResultsReply {
    bool success = false;
    std::string error_msg;
    std::vector<executor::Tuple> rows;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        out.push_back(success ? 1 : 0);

        const auto err_len = static_cast<uint32_t>(error_msg.size());
        size_t offset = out.size();
        out.resize(offset + 4 + err_len);
        std::memcpy(out.data() + offset, &err_len, 4);
        std::memcpy(out.data() + offset + 4, error_msg.data(), err_len);

        const auto row_count = static_cast<uint32_t>(rows.size());
        offset = out.size();
        out.resize(offset + 4);
        std::memcpy(out.data() + offset, &row_count, 4);

        for (const auto& row : rows) {
            Serializer::serialize_tuple(row, out);
        }

        return out;
    }

    static QueryResultsReply deserialize(const std::vector<uint8_t>& in) {
        QueryResultsReply reply;
        if (in.empty()) {
            return reply;
        }

        reply.success = in[0] != 0;

        size_t offset = 1;
        uint32_t err_len = 0;
        if (offset + 4 <= in.size()) {
            std::memcpy(&err_len, in.data() + offset, 4);
            offset += 4;
        }
        if (in.size() >= offset + err_len) {
            reply.error_msg = std::string(reinterpret_cast<const char*>(in.data() + offset), err_len);
            offset += err_len;
        }

        uint32_t row_count = 0;
        if (offset + 4 <= in.size()) {
            std::memcpy(&row_count, in.data() + offset, 4);
            offset += 4;
        }
        for (uint32_t i = 0; i < row_count; ++i) {
            reply.rows.push_back(Serializer::deserialize_tuple(in.data(), offset, in.size()));
        }

        return reply;
    }
};

/**
 * @brief Payload for pushing data between nodes (Shuffle)
 */
struct PushDataArgs {
    std::string table_name;
    std::vector<executor::Tuple> rows;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out;
        const auto name_len = static_cast<uint32_t>(table_name.size());
        out.resize(4 + name_len);
        std::memcpy(out.data(), &name_len, 4);
        std::memcpy(out.data() + 4, table_name.data(), name_len);

        const auto row_count = static_cast<uint32_t>(rows.size());
        const size_t offset = out.size();
        out.resize(offset + 4);
        std::memcpy(out.data() + offset, &row_count, 4);
        for (const auto& row : rows) {
            Serializer::serialize_tuple(row, out);
        }
        return out;
    }

    static PushDataArgs deserialize(const std::vector<uint8_t>& in) {
        PushDataArgs args;
        if (in.size() < 4) {
            return args;
        }
        uint32_t name_len = 0;
        size_t offset = 0;
        std::memcpy(&name_len, in.data() + offset, 4);
        offset += 4;
        if (in.size() >= offset + name_len) {
            args.table_name = std::string(reinterpret_cast<const char*>(in.data() + offset), name_len);
            offset += name_len;
        }
        uint32_t row_count = 0;
        if (offset + 4 <= in.size()) {
            std::memcpy(&row_count, in.data() + offset, 4);
            offset += 4;
        }
        for (uint32_t i = 0; i < row_count; ++i) {
            args.rows.push_back(Serializer::deserialize_tuple(in.data(), offset, in.size()));
        }
        return args;
    }
};

/**
 * @brief Payload for 2PC Operations (Prepare, Commit, Abort)
 */
struct TxnOperationArgs {
    uint64_t txn_id = 0;

    [[nodiscard]] std::vector<uint8_t> serialize() const {
        std::vector<uint8_t> out(8);
        std::memcpy(out.data(), &txn_id, 8);
        return out;
    }

    static TxnOperationArgs deserialize(const std::vector<uint8_t>& in) {
        TxnOperationArgs args;
        if (in.size() >= 8) {
            std::memcpy(&args.txn_id, in.data(), 8);
        }
        return args;
    }
};

/**
 * @brief Base class for RPC Payloads
 */
class RpcMessage {
   public:
    virtual ~RpcMessage() = default;
    [[nodiscard]] virtual RpcType type() const = 0;
    [[nodiscard]] virtual std::vector<uint8_t> serialize() const = 0;

    RpcMessage() = default;
    RpcMessage(const RpcMessage&) = default;
    RpcMessage& operator=(const RpcMessage&) = default;
    RpcMessage(RpcMessage&&) = default;
    RpcMessage& operator=(RpcMessage&&) = default;
};

}  // namespace cloudsql::network

#endif  // SQL_ENGINE_NETWORK_RPC_MESSAGE_HPP
