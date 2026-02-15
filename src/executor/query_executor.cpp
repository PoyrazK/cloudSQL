/**
 * @file query_executor.cpp
 * @brief High-level query executor implementation
 */

#include "executor/query_executor.hpp"
#include <chrono>
#include <algorithm>
#include <iostream>

namespace cloudsql {
namespace executor {

QueryExecutor::QueryExecutor(Catalog& catalog, 
                             storage::StorageManager& storage_manager,
                             transaction::LockManager& lock_manager,
                             transaction::TransactionManager& transaction_manager)
    : catalog_(catalog), storage_manager_(storage_manager), lock_manager_(lock_manager), transaction_manager_(transaction_manager) {}

QueryResult QueryExecutor::execute(const parser::Statement& stmt) {
    auto start = std::chrono::high_resolution_clock::now();
    QueryResult result;

    /* Handle Explicit Transaction Control */
    if (stmt.type() == parser::StmtType::TransactionBegin) {
        return execute_begin();
    } else if (stmt.type() == parser::StmtType::TransactionCommit) {
        return execute_commit();
    } else if (stmt.type() == parser::StmtType::TransactionRollback) {
        return execute_rollback();
    }

    /* Auto-commit mode if no current transaction */
    bool is_auto_commit = (current_txn_ == nullptr);
    transaction::Transaction* txn = current_txn_;
    
    if (is_auto_commit && (stmt.type() == parser::StmtType::Select || stmt.type() == parser::StmtType::Insert || 
                           stmt.type() == parser::StmtType::Update || stmt.type() == parser::StmtType::Delete)) {
        txn = transaction_manager_.begin();
    }

    try {
        if (stmt.type() == parser::StmtType::Select) {
            result = execute_select(static_cast<const parser::SelectStatement&>(stmt), txn);
        } else if (stmt.type() == parser::StmtType::CreateTable) {
            result = execute_create_table(static_cast<const parser::CreateTableStatement&>(stmt));
        } else if (stmt.type() == parser::StmtType::Insert) {
            result = execute_insert(static_cast<const parser::InsertStatement&>(stmt), txn);
        } else if (stmt.type() == parser::StmtType::Delete) {
            result = execute_delete(static_cast<const parser::DeleteStatement&>(stmt), txn);
        } else if (stmt.type() == parser::StmtType::Update) {
            result = execute_update(static_cast<const parser::UpdateStatement&>(stmt), txn);
        } else {
            result.set_error("Unsupported statement type");
        }

        /* Auto-commit success */
        if (is_auto_commit && txn) {
            transaction_manager_.commit(txn);
        }
    } catch (const std::exception& e) {
        if (is_auto_commit && txn) transaction_manager_.abort(txn);
        result.set_error(std::string("Execution error: ") + e.what());
    } catch (...) {
        if (is_auto_commit && txn) transaction_manager_.abort(txn);
        result.set_error("Unknown execution error");
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
    result.set_execution_time(duration.count());
    
    return result;
}

QueryResult QueryExecutor::execute_begin() {
    QueryResult res;
    if (current_txn_) {
        res.set_error("Transaction already in progress");
        return res;
    }
    current_txn_ = transaction_manager_.begin();
    return res;
}

QueryResult QueryExecutor::execute_commit() {
    QueryResult res;
    if (!current_txn_) {
        res.set_error("No transaction in progress");
        return res;
    }
    transaction_manager_.commit(current_txn_);
    current_txn_ = nullptr;
    return res;
}

QueryResult QueryExecutor::execute_rollback() {
    QueryResult res;
    if (!current_txn_) {
        res.set_error("No transaction in progress");
        return res;
    }
    transaction_manager_.abort(current_txn_);
    current_txn_ = nullptr;
    return res;
}

QueryResult QueryExecutor::execute_select(const parser::SelectStatement& stmt, transaction::Transaction* txn) {
    QueryResult result;
    
    /* Build execution plan */
    auto root = build_plan(stmt, txn);
    if (!root) {
        result.set_error("Failed to build execution plan (check table existence and FROM clause)");
        return result;
    }

    /* Initialize and open operators */
    if (!root->init() || !root->open()) {
        result.set_error(root->error().empty() ? "Failed to open execution plan" : root->error());
        return result;
    }

    /* Set result schema */
    result.set_schema(root->output_schema());

    /* Pull tuples (Volcano model) */
    Tuple tuple;
    while (root->next(tuple)) {
        result.add_row(std::move(tuple));
    }

    root->close();
    return result;
}

QueryResult QueryExecutor::execute_create_table(const parser::CreateTableStatement& stmt) {
    QueryResult result;
    
    /* Convert parser columns to catalog columns */
    std::vector<ColumnInfo> catalog_cols;
    uint16_t pos = 0;
    for (const auto& col : stmt.columns()) {
        common::ValueType type = common::TYPE_TEXT;
        if (col.type_ == "INT" || col.type_ == "INTEGER") type = common::TYPE_INT32;
        else if (col.type_ == "BIGINT") type = common::TYPE_INT64;
        else if (col.type_ == "FLOAT" || col.type_ == "DOUBLE") type = common::TYPE_FLOAT64;
        else if (col.type_ == "BOOLEAN" || col.type_ == "BOOL") type = common::TYPE_BOOL;
        
        catalog_cols.emplace_back(col.name_, type, pos++);
    }

    /* Update catalog */
    oid_t table_id = catalog_.create_table(stmt.table_name(), std::move(catalog_cols));
    if (table_id == 0) {
        result.set_error("Failed to create table in catalog");
        return result;
    }

    /* Create physical file */
    auto table_info = catalog_.get_table(table_id);
    storage::HeapTable table((*table_info)->name, storage_manager_, executor::Schema());
    if (!table.create()) {
        catalog_.drop_table(table_id);
        result.set_error("Failed to create table file");
        return result;
    }

    result.set_rows_affected(1);
    return result;
}

QueryResult QueryExecutor::execute_insert(const parser::InsertStatement& stmt, transaction::Transaction* txn) {
    QueryResult result;
    
    if (!stmt.table()) {
        result.set_error("Target table not specified");
        return result;
    }

    std::string table_name = stmt.table()->to_string();
    auto table_meta = catalog_.get_table_by_name(table_name);
    if (!table_meta) {
        result.set_error("Table not found: " + table_name);
        return result;
    }

    /* Construct Schema */
    Schema schema;
    for (const auto& col : (*table_meta)->columns) {
        schema.add_column(col.name, col.type);
    }

    storage::HeapTable table(table_name, storage_manager_, schema);
    
    uint64_t rows_inserted = 0;
    uint64_t xmin = txn ? txn->get_id() : 0;

    for (const auto& row_exprs : stmt.values()) {
        std::vector<common::Value> values;
        for (const auto& expr : row_exprs) {
            values.push_back(expr->evaluate());
        }
        
        auto tid = table.insert(Tuple(std::move(values)), xmin);
        
        /* Record undo log and Acquire Exclusive Lock if in transaction */
        if (txn) {
            txn->add_undo_log(transaction::UndoLog::Type::INSERT, table_name, tid);
            if (!lock_manager_.acquire_exclusive(txn, std::to_string(tid.page_num) + ":" + std::to_string(tid.slot_num))) {
                throw std::runtime_error("Failed to acquire exclusive lock");
            }
        }

        rows_inserted++;
    }

    result.set_rows_affected(rows_inserted);
    return result;
}

QueryResult QueryExecutor::execute_delete(const parser::DeleteStatement& stmt, transaction::Transaction* txn) {
    QueryResult result;
    std::string table_name = stmt.table()->to_string();
    auto table_meta = catalog_.get_table_by_name(table_name);
    if (!table_meta) {
        result.set_error("Table not found: " + table_name);
        return result;
    }

    Schema schema;
    for (const auto& col : (*table_meta)->columns) {
        schema.add_column(col.name, col.type);
    }

    storage::HeapTable table(table_name, storage_manager_, schema);
    uint64_t xmax = txn ? txn->get_id() : 0;
    uint64_t rows_deleted = 0;

    /* Phase 1: Collect RIDs to avoid Halloween Problem */
    std::vector<storage::HeapTable::TupleId> target_rids;
    auto iter = table.scan();
    storage::HeapTable::TupleMeta meta;
    while (iter.next_meta(meta)) {
        bool match = true;
        if (stmt.where()) {
            match = stmt.where()->evaluate(&meta.tuple, &schema).as_bool();
        }

        if (match && meta.xmax == 0) {
            target_rids.push_back(iter.current_id());
        }
    }

    /* Phase 2: Apply Deletions */
    for (const auto& rid : target_rids) {
        if (table.remove(rid, xmax)) {
            if (txn) {
                txn->add_undo_log(transaction::UndoLog::Type::DELETE, table_name, rid);
            }
            rows_deleted++;
        }
    }

    result.set_rows_affected(rows_deleted);
    return result;
}

QueryResult QueryExecutor::execute_update(const parser::UpdateStatement& stmt, transaction::Transaction* txn) {
    QueryResult result;
    std::string table_name = stmt.table()->to_string();
    auto table_meta = catalog_.get_table_by_name(table_name);
    if (!table_meta) {
        result.set_error("Table not found: " + table_name);
        return result;
    }

    Schema schema;
    for (const auto& col : (*table_meta)->columns) {
        schema.add_column(col.name, col.type);
    }

    storage::HeapTable table(table_name, storage_manager_, schema);
    uint64_t txn_id = txn ? txn->get_id() : 0;
    uint64_t rows_updated = 0;

    /* Phase 1: Collect RIDs and compute new values to avoid Halloween Problem */
    struct UpdateOp {
        storage::HeapTable::TupleId rid;
        Tuple new_tuple;
    };
    std::vector<UpdateOp> updates;

    auto iter = table.scan();
    storage::HeapTable::TupleMeta meta;
    while (iter.next_meta(meta)) {
        bool match = true;
        if (stmt.where()) {
            match = stmt.where()->evaluate(&meta.tuple, &schema).as_bool();
        }

        if (match && meta.xmax == 0) {
            /* Compute new tuple values */
            Tuple new_tuple = meta.tuple;
            for (const auto& [col_expr, val_expr] : stmt.set_clauses()) {
                std::string col_name = col_expr->to_string();
                size_t idx = schema.find_column(col_name);
                if (idx != static_cast<size_t>(-1)) {
                    new_tuple.set(idx, val_expr->evaluate(&meta.tuple, &schema));
                }
            }
            updates.push_back({iter.current_id(), std::move(new_tuple)});
        }
    }

    /* Phase 2: Apply Updates */
    for (const auto& op : updates) {
        if (table.remove(op.rid, txn_id)) {
            auto new_tid = table.insert(op.new_tuple, txn_id);
            if (txn) {
                txn->add_undo_log(transaction::UndoLog::Type::UPDATE, table_name, op.rid);
                txn->add_undo_log(transaction::UndoLog::Type::INSERT, table_name, new_tid);
            }
            rows_updated++;
        }
    }

    result.set_rows_affected(rows_updated);
    return result;
}

std::unique_ptr<Operator> QueryExecutor::build_plan(const parser::SelectStatement& stmt, transaction::Transaction* txn) {
    /* 1. Base: SeqScan */
    if (!stmt.from()) return nullptr;
    
    std::string table_name = stmt.from()->to_string();
    auto table_meta = catalog_.get_table_by_name(table_name);
    if (!table_meta) return nullptr;

    /* Construct Schema for HeapTable */
    Schema schema;
    for (const auto& col : (*table_meta)->columns) {
        schema.add_column(col.name, col.type);
    }

    auto scan = std::make_unique<SeqScanOperator>(
        std::make_unique<storage::HeapTable>(table_name, storage_manager_, schema),
        txn, &lock_manager_
    );

    std::unique_ptr<Operator> current_root = std::move(scan);

    /* 2. Filter (WHERE) */
    if (stmt.where()) {
        current_root = std::make_unique<FilterOperator>(
            std::move(current_root),
            stmt.where()->clone()
        );
    }

    /* 3. Aggregate (GROUP BY or implicit aggregates) */
    bool has_aggregates = false;
    std::vector<AggregateInfo> aggs;
    for (const auto& col : stmt.columns()) {
        if (col->type() == parser::ExprType::Function) {
            auto func = static_cast<const parser::FunctionExpr*>(col.get());
            std::string name = func->name();
            std::transform(name.begin(), name.end(), name.begin(), ::toupper);
            
            if (name == "COUNT" || name == "SUM" || name == "MIN" || name == "MAX" || name == "AVG") {
                has_aggregates = true;
                AggregateType type = AggregateType::Count;
                if (name == "SUM") type = AggregateType::Sum;
                else if (name == "MIN") type = AggregateType::Min;
                else if (name == "MAX") type = AggregateType::Max;
                else if (name == "AVG") type = AggregateType::Avg;

                AggregateInfo info;
                info.type = type;
                info.expr = (!func->args().empty()) ? func->args()[0]->clone() : nullptr;
                info.is_distinct = func->distinct();
                
                /* Normalize aggregate name for schema lookup */
                std::string agg_name = name + "(";
                if (info.is_distinct) agg_name += "DISTINCT ";
                agg_name += (info.expr ? info.expr->to_string() : "*") + ")";
                info.name = agg_name;
                
                aggs.push_back(std::move(info));
            }
        }
    }

    if (!stmt.group_by().empty() || has_aggregates) {
        std::vector<std::unique_ptr<parser::Expression>> group_by;
        for (const auto& gb : stmt.group_by()) {
            group_by.push_back(gb->clone());
        }
        current_root = std::make_unique<AggregateOperator>(
            std::move(current_root),
            std::move(group_by),
            std::move(aggs)
        );
    }

    /* 4. Sort (ORDER BY) */
    if (!stmt.order_by().empty()) {
        std::vector<std::unique_ptr<parser::Expression>> sort_keys;
        std::vector<bool> ascending;
        for (const auto& ob : stmt.order_by()) {
            sort_keys.push_back(ob->clone());
            ascending.push_back(true); /* Default to ASC */
        }
        current_root = std::make_unique<SortOperator>(
            std::move(current_root),
            std::move(sort_keys),
            std::move(ascending)
        );
    }

    /* 5. Project (SELECT columns) */
    if (!stmt.columns().empty()) {
        std::vector<std::unique_ptr<parser::Expression>> projection;
        for (const auto& col : stmt.columns()) {
            projection.push_back(col->clone());
        }
        current_root = std::make_unique<ProjectOperator>(
            std::move(current_root),
            std::move(projection)
        );
    }

    /* 6. Limit */
    if (stmt.has_limit() || stmt.has_offset()) {
        current_root = std::make_unique<LimitOperator>(
            std::move(current_root),
            stmt.limit(),
            stmt.offset()
        );
    }

    return current_root;
}

} // namespace executor
} // namespace cloudsql
