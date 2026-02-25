/**
 * @file recovery_manager.hpp
 * @brief Recovery Manager for system crash recovery
 */

#ifndef CLOUDSQL_RECOVERY_RECOVERY_MANAGER_HPP
#define CLOUDSQL_RECOVERY_RECOVERY_MANAGER_HPP

#include <string>
#include <unordered_map>
#include <vector>

#include "catalog/catalog.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/log_record.hpp"
#include "storage/buffer_pool_manager.hpp"

namespace cloudsql::recovery {

/**
 * @class RecoveryManager
 * @brief Manages ARIES-style crash recovery (Analysis, Redo, Undo)
 */
class RecoveryManager {
   public:
    RecoveryManager(storage::BufferPoolManager& bpm, Catalog& catalog, LogManager& log_manager)
        : bpm_(bpm), catalog_(catalog), log_manager_(log_manager){}

    ~RecoveryManager() = default;

    // Rule of Five: explicitly handle copy/move
    RecoveryManager(const RecoveryManager&) = delete;
    RecoveryManager& operator=(const RecoveryManager&) = delete;
    RecoveryManager(RecoveryManager&&) = delete;
    RecoveryManager& operator=(RecoveryManager&&) = delete;

    /**
     * @brief Perform crash recovery
     * @return true if successful
     */
    bool recover();

   private:
    void analyze();
    void redo();
    void undo();

    storage::BufferPoolManager& bpm_;
    Catalog& catalog_;
    LogManager& log_manager_;

    // Recovery states
    std::unordered_map<txn_id_t, lsn_t> active_txns_;
    lsn_t max_lsn_{0};
};

}  // namespace cloudsql::recovery

#endif  // CLOUDSQL_RECOVERY_RECOVERY_MANAGER_HPP
