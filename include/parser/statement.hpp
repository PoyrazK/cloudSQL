/**
 * @file statement.hpp
 * @brief Statement classes for SQL AST
 */

#ifndef CLOUDSQL_PARSER_STATEMENT_HPP
#define CLOUDSQL_PARSER_STATEMENT_HPP

#include <memory>
#include <string>
#include <vector>
#include "parser/expression.hpp"

namespace cloudsql {
namespace parser {

/**
 * @brief Statement types
 */
enum class StmtType {
    Select,
    Insert,
    Update,
    Delete,
    CreateTable,
    DropTable,
    AlterTable,
    CreateIndex,
    DropIndex,
    TransactionBegin,
    TransactionCommit,
    TransactionRollback,
    Explain
};

/**
 * @brief Base statement class
 */
class Statement {
public:
    virtual ~Statement() = default;
    virtual StmtType type() const = 0;
    virtual std::string to_string() const = 0;
};

/**
 * @brief SELECT statement
 */
class SelectStatement : public Statement {
private:
    std::vector<std::unique_ptr<Expression>> columns_;
    std::unique_ptr<Expression> from_;
    std::unique_ptr<Expression> where_;
    std::vector<std::unique_ptr<Expression>> group_by_;
    std::unique_ptr<Expression> having_;
    std::vector<std::unique_ptr<Expression>> order_by_;
    int64_t limit_ = 0;
    int64_t offset_ = 0;
    bool distinct_ = false;
    
public:
    SelectStatement() = default;
    
    StmtType type() const override { return StmtType::Select; }
    
    void add_column(std::unique_ptr<Expression> col) {
        columns_.push_back(std::move(col));
    }
    void add_from(std::unique_ptr<Expression> table) {
        from_ = std::move(table);
    }
    void set_where(std::unique_ptr<Expression> where) {
        where_ = std::move(where);
    }
    void add_group_by(std::unique_ptr<Expression> expr) {
        group_by_.push_back(std::move(expr));
    }
    void set_having(std::unique_ptr<Expression> having) {
        having_ = std::move(having);
    }
    void add_order_by(std::unique_ptr<Expression> expr) {
        order_by_.push_back(std::move(expr));
    }
    void set_limit(int64_t limit) { limit_ = limit; }
    void set_offset(int64_t offset) { offset_ = offset; }
    void set_distinct(bool distinct) { distinct_ = distinct; }
    
    const auto& columns() const { return columns_; }
    const Expression* from() const { return from_.get(); }
    const Expression* where() const { return where_.get(); }
    const auto& group_by() const { return group_by_; }
    const Expression* having() const { return having_.get(); }
    const auto& order_by() const { return order_by_; }
    int64_t limit() const { return limit_; }
    int64_t offset() const { return offset_; }
    bool distinct() const { return distinct_; }
    bool has_limit() const { return limit_ > 0; }
    bool has_offset() const { return offset_ > 0; }
    
    std::string to_string() const override;
};

/**
 * @brief INSERT statement
 */
class InsertStatement : public Statement {
private:
    std::unique_ptr<Expression> table_;
    std::vector<std::unique_ptr<Expression>> columns_;
    std::vector<std::vector<std::unique_ptr<Expression>>> values_;
    
public:
    explicit InsertStatement() = default;
    
    StmtType type() const override { return StmtType::Insert; }
    
    void set_table(std::unique_ptr<Expression> table) {
        table_ = std::move(table);
    }
    void add_column(std::unique_ptr<Expression> col) {
        columns_.push_back(std::move(col));
    }
    void add_row(std::vector<std::unique_ptr<Expression>> row) {
        values_.push_back(std::move(row));
    }
    
    const Expression* table() const { return table_.get(); }
    const auto& columns() const { return columns_; }
    const auto& values() const { return values_; }
    size_t value_count() const { return values_.size(); }
    
    std::string to_string() const override;
};

/**
 * @brief UPDATE statement
 */
class UpdateStatement : public Statement {
private:
    std::unique_ptr<Expression> table_;
    std::vector<std::pair<std::unique_ptr<Expression>, std::unique_ptr<Expression>>> set_clauses_;
    std::unique_ptr<Expression> where_;
    
public:
    explicit UpdateStatement() = default;
    
    StmtType type() const override { return StmtType::Update; }
    
    void set_table(std::unique_ptr<Expression> table) {
        table_ = std::move(table);
    }
    void add_set(std::unique_ptr<Expression> col, std::unique_ptr<Expression> val) {
        set_clauses_.push_back({std::move(col), std::move(val)});
    }
    void set_where(std::unique_ptr<Expression> where) {
        where_ = std::move(where);
    }
    
    const Expression* table() const { return table_.get(); }
    const auto& set_clauses() const { return set_clauses_; }
    const Expression* where() const { return where_.get(); }
    
    std::string to_string() const override;
};

/**
 * @brief DELETE statement
 */
class DeleteStatement : public Statement {
private:
    std::unique_ptr<Expression> table_;
    std::unique_ptr<Expression> where_;
    
public:
    explicit DeleteStatement() = default;
    
    StmtType type() const override { return StmtType::Delete; }
    
    void set_table(std::unique_ptr<Expression> table) {
        table_ = std::move(table);
    }
    void set_where(std::unique_ptr<Expression> where) {
        where_ = std::move(where);
    }
    
    const Expression* table() const { return table_.get(); }
    const Expression* where() const { return where_.get(); }
    bool has_where() const { return where_ != nullptr; }
    
    std::string to_string() const override;
};

/**
 * @brief CREATE TABLE statement
 */
class CreateTableStatement : public Statement {
private:
    std::string table_name_;
    struct ColumnDef {
        std::string name_;
        std::string type_;
        bool is_primary_key_ = false;
        bool is_not_null_ = false;
        bool is_unique_ = false;
        std::unique_ptr<Expression> default_value_;
    };
    std::vector<ColumnDef> columns_;
    
public:
    explicit CreateTableStatement() = default;
    
    StmtType type() const override { return StmtType::CreateTable; }
    
    void set_table_name(std::string name) { table_name_ = std::move(name); }
    void add_column(std::string name, std::string type) {
        columns_.push_back({std::move(name), std::move(type), false, false, false, nullptr});
    }
    ColumnDef& get_last_column() { return columns_.back(); }
    
    const std::string& table_name() const { return table_name_; }
    const auto& columns() const { return columns_; }
    
    std::string to_string() const override;
};

// Inline implementations

inline std::string SelectStatement::to_string() const {
    std::string result = "SELECT ";
    
    if (distinct_) result += "DISTINCT ";
    
    bool first = true;
    for (const auto& col : columns_) {
        if (!first) result += ", ";
        result += col->to_string();
        first = false;
    }
    
    if (from_) {
        result += " FROM " + from_->to_string();
    }
    
    if (where_) {
        result += " WHERE " + where_->to_string();
    }
    
    if (!group_by_.empty()) {
        result += " GROUP BY ";
        first = true;
        for (const auto& expr : group_by_) {
            if (!first) result += ", ";
            result += expr->to_string();
            first = false;
        }
    }
    
    if (having_) {
        result += " HAVING " + having_->to_string();
    }
    
    if (!order_by_.empty()) {
        result += " ORDER BY ";
        first = true;
        for (const auto& expr : order_by_) {
            if (!first) result += ", ";
            result += expr->to_string();
            first = false;
        }
    }
    
    if (has_limit()) {
        result += " LIMIT " + std::to_string(limit_);
    }
    
    if (has_offset()) {
        result += " OFFSET " + std::to_string(offset_);
    }
    
    return result;
}

inline std::string InsertStatement::to_string() const {
    std::string result = "INSERT INTO " + table_->to_string() + " (";
    
    bool first = true;
    for (const auto& col : columns_) {
        if (!first) result += ", ";
        result += col->to_string();
        first = false;
    }
    result += ") VALUES ";
    
    first = true;
    for (const auto& row : values_) {
        if (!first) result += ", ";
        result += "(";
        bool inner_first = true;
        for (const auto& val : row) {
            if (!inner_first) result += ", ";
            result += val->to_string();
            inner_first = false;
        }
        result += ")";
        first = false;
    }
    
    return result;
}

inline std::string UpdateStatement::to_string() const {
    std::string result = "UPDATE " + table_->to_string() + " SET ";
    
    bool first = true;
    for (const auto& [col, val] : set_clauses_) {
        if (!first) result += ", ";
        result += col->to_string() + " = " + val->to_string();
        first = false;
    }
    
    if (where_) {
        result += " WHERE " + where_->to_string();
    }
    
    return result;
}

inline std::string DeleteStatement::to_string() const {
    std::string result = "DELETE FROM " + table_->to_string();
    
    if (where_) {
        result += " WHERE " + where_->to_string();
    }
    
    return result;
}

inline std::string CreateTableStatement::to_string() const {
    std::string result = "CREATE TABLE " + table_name_ + " (";
    
    bool first = true;
    for (const auto& col : columns_) {
        if (!first) result += ", ";
        result += col.name_ + " " + col.type_;
        if (col.is_primary_key_) result += " PRIMARY KEY";
        if (col.is_not_null_) result += " NOT NULL";
        if (col.is_unique_) result += " UNIQUE";
        first = false;
    }
    
    result += ")";
    return result;
}

}  // namespace parser
}  // namespace cloudsql

#endif  // CLOUDSQL_PARSER_STATEMENT_HPP
