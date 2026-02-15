/**
 * @file query_executor.hpp
 * @brief High-level query executor
 */

#ifndef CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP
#define CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP

#include "parser/statement.hpp"
#include "executor/types.hpp"
#include "executor/operator.hpp"
#include "catalog/catalog.hpp"
#include "storage/storage_manager.hpp"
#include "transaction/transaction_manager.hpp"

namespace cloudsql {
namespace executor {

/**
 * @brief Top-level executor that coordinates planning and operator execution
 */
class QueryExecutor {
public:
    QueryExecutor(Catalog& catalog, 
                  storage::StorageManager& storage_manager,
                  transaction::LockManager& lock_manager,
                  transaction::TransactionManager& transaction_manager);
    ~QueryExecutor() = default;

    /**
     * @brief Execute a SQL statement and return results
     */
    QueryResult execute(const parser::Statement& stmt);

private:
    Catalog& catalog_;
    storage::StorageManager& storage_manager_;
    transaction::LockManager& lock_manager_;
    transaction::TransactionManager& transaction_manager_;
    transaction::Transaction* current_txn_ = nullptr;

    QueryResult execute_select(const parser::SelectStatement& stmt, transaction::Transaction* txn);
    QueryResult execute_create_table(const parser::CreateTableStatement& stmt);
    QueryResult execute_insert(const parser::InsertStatement& stmt, transaction::Transaction* txn);
    QueryResult execute_update(const parser::UpdateStatement& stmt, transaction::Transaction* txn);
    QueryResult execute_delete(const parser::DeleteStatement& stmt, transaction::Transaction* txn);
    
    /* Transaction control */
    QueryResult execute_begin();
    QueryResult execute_commit();
    QueryResult execute_rollback();

    /* Helper to build operator tree from SELECT */
    std::unique_ptr<Operator> build_plan(const parser::SelectStatement& stmt, transaction::Transaction* txn);
};

} // namespace executor
} // namespace cloudsql

#endif // CLOUDSQL_EXECUTOR_QUERY_EXECUTOR_HPP
