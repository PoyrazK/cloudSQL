/**
 * @file operator.cpp
 * @brief Query Executor Operators implementation
 *
 * @defgroup executor Query Executor
 * @{
 */

#include "executor/operator.hpp"

namespace cloudsql {
namespace executor {

/* --- SeqScanOperator --- */

SeqScanOperator::SeqScanOperator(std::unique_ptr<storage::HeapTable> table)
    : Operator(OperatorType::SeqScan)
    , table_name_(table->table_name())
    , table_(std::move(table))
    , schema_(table_->schema()) {}

bool SeqScanOperator::init() {
    state_ = ExecState::Init;
    return true;
}

bool SeqScanOperator::open() {
    state_ = ExecState::Open;
    iterator_ = std::make_unique<storage::HeapTable::Iterator>(table_->scan());
    return true;
}

bool SeqScanOperator::next(Tuple& out_tuple) {
    if (!iterator_ || iterator_->is_done()) {
        state_ = ExecState::Done;
        return false;
    }
    
    if (iterator_->next(out_tuple)) {
        return true;
    }
    
    state_ = ExecState::Done;
    return false;
}

void SeqScanOperator::close() {
    iterator_.reset();
    state_ = ExecState::Done;
}

Schema& SeqScanOperator::output_schema() { return schema_; }

/* --- FilterOperator --- */

FilterOperator::FilterOperator(std::unique_ptr<Operator> child, std::unique_ptr<parser::Expression> condition)
    : Operator(OperatorType::Filter), child_(std::move(child)), condition_(std::move(condition)) {
    if (child_) schema_ = child_->output_schema();
}

bool FilterOperator::init() {
    return child_->init();
}

bool FilterOperator::open() {
    if (!child_->open()) return false;
    state_ = ExecState::Open;
    return true;
}

bool FilterOperator::next(Tuple& out_tuple) {
    Tuple tuple;
    while (child_->next(tuple)) {
        /* Evaluate condition against the current tuple */
        /* TODO: Bind tuple to expression context for proper evaluation */
        common::Value result = condition_->evaluate();
        if (result.as_bool()) {
            out_tuple = std::move(tuple);
            return true;
        }
    }
    state_ = ExecState::Done;
    return false;
}

void FilterOperator::close() {
    child_->close();
    state_ = ExecState::Done;
}

Schema& FilterOperator::output_schema() { return schema_; }

void FilterOperator::add_child(std::unique_ptr<Operator> child) { child_ = std::move(child); }

/* --- ProjectOperator --- */

ProjectOperator::ProjectOperator(std::unique_ptr<Operator> child, std::vector<std::unique_ptr<parser::Expression>> columns)
    : Operator(OperatorType::Project), child_(std::move(child)), columns_(std::move(columns)) {
}

bool ProjectOperator::init() {
    return child_->init();
}

bool ProjectOperator::open() {
    if (!child_->open()) return false;
    state_ = ExecState::Open;
    return true;
}

bool ProjectOperator::next(Tuple& out_tuple) {
    Tuple input;
    if (!child_->next(input)) {
        state_ = ExecState::Done;
        return false;
    }
    
    std::vector<common::Value> output_values;
    for (const auto& col : columns_) {
        /* Evaluate projection expression */
        output_values.push_back(col->evaluate());
    }
    out_tuple = Tuple(std::move(output_values));
    return true;
}

void ProjectOperator::close() {
    child_->close();
    state_ = ExecState::Done;
}

Schema& ProjectOperator::output_schema() { return schema_; }

void ProjectOperator::add_child(std::unique_ptr<Operator> child) { child_ = std::move(child); }

/* --- HashJoinOperator --- */

HashJoinOperator::HashJoinOperator(std::unique_ptr<Operator> left, std::unique_ptr<Operator> right,
                     std::unique_ptr<parser::Expression> left_key, 
                     std::unique_ptr<parser::Expression> right_key)
    : Operator(OperatorType::HashJoin), left_(std::move(left)), right_(std::move(right)),
      left_key_(std::move(left_key)), right_key_(std::move(right_key)) {}

bool HashJoinOperator::init() {
    return left_->init() && right_->init();
}

bool HashJoinOperator::open() {
    /* Build phase: scan right side into hash table */
    Tuple right_tuple;
    while (right_->next(right_tuple)) {
        // Build hash table entries
    }
    state_ = ExecState::Open;
    return true;
}

bool HashJoinOperator::next(Tuple& out_tuple) {
    if (current_index_ >= results_.size()) {
        state_ = ExecState::Done;
        return false;
    }
    /* Probe phase: return joined results */
    out_tuple = results_[current_index_++];
    return true;
}

void HashJoinOperator::close() {
    left_->close();
    right_->close();
    state_ = ExecState::Done;
}

Schema& HashJoinOperator::output_schema() { return schema_; }

void HashJoinOperator::add_child(std::unique_ptr<Operator> child) {
    if (!left_) {
        left_ = std::move(child);
    } else {
        right_ = std::move(child);
    }
}

/* --- LimitOperator --- */

LimitOperator::LimitOperator(std::unique_ptr<Operator> child, uint64_t limit, uint64_t offset)
    : Operator(OperatorType::Limit), child_(std::move(child)), limit_(limit), offset_(offset) {}

bool LimitOperator::init() {
    return child_->init();
}

bool LimitOperator::open() {
    if (!child_->open()) return false;
    
    /* Skip offset rows */
    Tuple tuple;
    while (current_count_ < offset_ && child_->next(tuple)) {
        current_count_++;
    }
    current_count_ = 0;
    state_ = ExecState::Open;
    return true;
}

bool LimitOperator::next(Tuple& out_tuple) {
    if (current_count_ >= limit_) {
        state_ = ExecState::Done;
        return false;
    }
    
    if (!child_->next(out_tuple)) {
        state_ = ExecState::Done;
        return false;
    }
    
    current_count_++;
    return true;
}

void LimitOperator::close() {
    child_->close();
    state_ = ExecState::Done;
}

Schema& LimitOperator::output_schema() { return child_->output_schema(); }

void LimitOperator::add_child(std::unique_ptr<Operator> child) { child_ = std::move(child); }

}  // namespace executor
}  // namespace cloudsql

/** @} */ /* executor */
