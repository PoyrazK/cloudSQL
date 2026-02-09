/**
 * @file expression.hpp
 * @brief Expression classes for SQL AST
 */

#ifndef CLOUDSQL_PARSER_EXPRESSION_HPP
#define CLOUDSQL_PARSER_EXPRESSION_HPP

#include <memory>
#include <string>
#include <vector>
#include "common/value.hpp"
#include "parser/token.hpp"

namespace cloudsql {
namespace parser {

class Expression;

enum class ExprType {
    Binary,
    Unary,
    Column,
    Constant,
    Function,
    Subquery,
    In,
    Like,
    Between,
    IsNull
};

class Expression {
public:
    virtual ~Expression() = default;
    virtual ExprType type() const = 0;
    virtual common::Value evaluate() const = 0;
    virtual std::string to_string() const = 0;
    virtual std::unique_ptr<Expression> clone() const = 0;
};

class BinaryExpr : public Expression {
private:
    std::unique_ptr<Expression> left_;
    TokenType op_;
    std::unique_ptr<Expression> right_;
    
public:
    BinaryExpr(std::unique_ptr<Expression> left, TokenType op, std::unique_ptr<Expression> right)
        : left_(std::move(left)), op_(op), right_(std::move(right)) {}
    
    ExprType type() const override { return ExprType::Binary; }
    const Expression& left() const { return *left_; }
    const Expression& right() const { return *right_; }
    TokenType op() const { return op_; }
    
    common::Value evaluate() const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class UnaryExpr : public Expression {
private:
    TokenType op_;
    std::unique_ptr<Expression> expr_;
    
public:
    UnaryExpr(TokenType op, std::unique_ptr<Expression> expr)
        : op_(op), expr_(std::move(expr)) {}
    
    ExprType type() const override { return ExprType::Unary; }
    TokenType op() const { return op_; }
    const Expression& expr() const { return *expr_; }
    
    common::Value evaluate() const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class ColumnExpr : public Expression {
private:
    std::string name_;
    std::string table_name_;
    
public:
    explicit ColumnExpr(std::string name)
        : name_(std::move(name)), table_name_() {}
    
    ColumnExpr(std::string table, std::string name)
        : table_name_(std::move(table)), name_(std::move(name)) {}
    
    ExprType type() const override { return ExprType::Column; }
    const std::string& name() const { return name_; }
    const std::string& table() const { return table_name_; }
    bool has_table() const { return !table_name_.empty(); }
    
    common::Value evaluate() const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class ConstantExpr : public Expression {
private:
    common::Value value_;
    
public:
    explicit ConstantExpr(common::Value value) : value_(std::move(value)) {}
    
    ExprType type() const override { return ExprType::Constant; }
    const common::Value& value() const { return value_; }
    
    common::Value evaluate() const override { return value_; }
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class FunctionExpr : public Expression {
private:
    std::string func_name_;
    std::vector<std::unique_ptr<Expression>> args_;
    
public:
    explicit FunctionExpr(std::string name)
        : func_name_(std::move(name)) {}
    
    ExprType type() const override { return ExprType::Function; }
    const std::string& name() const { return func_name_; }
    void add_arg(std::unique_ptr<Expression> arg) { args_.push_back(std::move(arg)); }
    const auto& args() const { return args_; }
    
    common::Value evaluate() const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class InExpr : public Expression {
private:
    std::unique_ptr<Expression> column_;
    std::vector<std::unique_ptr<Expression>> values_;
    bool not_flag_;
    
public:
    InExpr(std::unique_ptr<Expression> column, std::vector<std::unique_ptr<Expression>> values, bool not_flag = false)
        : column_(std::move(column)), values_(std::move(values)), not_flag_(not_flag) {}
    
    ExprType type() const override { return ExprType::In; }
    const Expression& column() const { return *column_; }
    const auto& values() const { return values_; }
    bool is_not() const { return not_flag_; }
    
    common::Value evaluate() const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

class IsNullExpr : public Expression {
private:
    std::unique_ptr<Expression> expr_;
    bool not_flag_;
    
public:
    IsNullExpr(std::unique_ptr<Expression> expr, bool not_flag = false)
        : expr_(std::move(expr)), not_flag_(not_flag) {}
    
    ExprType type() const override { return ExprType::IsNull; }
    const Expression& expr() const { return *expr_; }
    bool is_not() const { return not_flag_; }
    
    common::Value evaluate() const override;
    std::string to_string() const override;
    std::unique_ptr<Expression> clone() const override;
};

// Inline implementations

inline common::Value BinaryExpr::evaluate() const {
    common::Value left_val = left_->evaluate();
    common::Value right_val = right_->evaluate();
    
    switch (op_) {
        case TokenType::Plus:
            if (left_val.type() == common::TYPE_FLOAT64 || right_val.type() == common::TYPE_FLOAT64) {
                return common::Value::make_float64(left_val.to_float64() + right_val.to_float64());
            }
            return common::Value::make_int64(left_val.to_int64() + right_val.to_int64());
        case TokenType::Minus:
            if (left_val.type() == common::TYPE_FLOAT64 || right_val.type() == common::TYPE_FLOAT64) {
                return common::Value::make_float64(left_val.to_float64() - right_val.to_float64());
            }
            return common::Value::make_int64(left_val.to_int64() - right_val.to_int64());
        case TokenType::Star:
            if (left_val.type() == common::TYPE_FLOAT64 || right_val.type() == common::TYPE_FLOAT64) {
                return common::Value::make_float64(left_val.to_float64() * right_val.to_float64());
            }
            return common::Value::make_int64(left_val.to_int64() * right_val.to_int64());
        case TokenType::Slash:
            return common::Value::make_float64(left_val.to_float64() / right_val.to_float64());
        case TokenType::Eq: return common::Value(left_val == right_val);
        case TokenType::Ne: return common::Value(left_val != right_val);
        case TokenType::Lt: return common::Value(left_val < right_val);
        case TokenType::Le: return common::Value(left_val <= right_val);
        case TokenType::Gt: return common::Value(left_val > right_val);
        case TokenType::Ge: return common::Value(left_val >= right_val);
        case TokenType::And: return common::Value(left_val.as_bool() && right_val.as_bool());
        case TokenType::Or: return common::Value(left_val.as_bool() || right_val.as_bool());
        default: return common::Value::make_null();
    }
}

inline std::string BinaryExpr::to_string() const {
    std::string op_str;
    switch (op_) {
        case TokenType::Plus: op_str = " + "; break;
        case TokenType::Minus: op_str = " - "; break;
        case TokenType::Star: op_str = " * "; break;
        case TokenType::Slash: op_str = " / "; break;
        case TokenType::Eq: op_str = " = "; break;
        case TokenType::Ne: op_str = " <> "; break;
        case TokenType::Lt: op_str = " < "; break;
        case TokenType::Le: op_str = " <= "; break;
        case TokenType::Gt: op_str = " > "; break;
        case TokenType::Ge: op_str = " >= "; break;
        case TokenType::And: op_str = " AND "; break;
        case TokenType::Or: op_str = " OR "; break;
        default: op_str = " "; break;
    }
    return left_->to_string() + op_str + right_->to_string();
}

inline std::unique_ptr<Expression> BinaryExpr::clone() const {
    return std::make_unique<BinaryExpr>(left_->clone(), op_, right_->clone());
}

inline common::Value UnaryExpr::evaluate() const {
    common::Value val = expr_->evaluate();
    switch (op_) {
        case TokenType::Minus:
            if (val.is_numeric()) {
                return common::Value(-val.to_float64());
            }
            break;
        case TokenType::Not:
            return common::Value(!val.as_bool());
        default: break;
    }
    return common::Value::make_null();
}

inline std::string UnaryExpr::to_string() const {
    return (op_ == TokenType::Minus ? "-" : "NOT ") + expr_->to_string();
}

inline std::unique_ptr<Expression> UnaryExpr::clone() const {
    return std::make_unique<UnaryExpr>(op_, expr_->clone());
}

inline common::Value ColumnExpr::evaluate() const {
    return common::Value::make_null();
}

inline std::string ColumnExpr::to_string() const {
    return has_table() ? table_name_ + "." + name_ : name_;
}

inline std::unique_ptr<Expression> ColumnExpr::clone() const {
    return has_table() 
        ? std::make_unique<ColumnExpr>(table_name_, name_)
        : std::make_unique<ColumnExpr>(name_);
}

inline std::string ConstantExpr::to_string() const {
    if (value_.type() == common::TYPE_TEXT) {
        return "'" + value_.to_string() + "'";
    }
    return value_.to_string();
}

inline std::unique_ptr<Expression> ConstantExpr::clone() const {
    return std::make_unique<ConstantExpr>(value_);
}

inline common::Value FunctionExpr::evaluate() const {
    return common::Value::make_null();
}

inline std::string FunctionExpr::to_string() const {
    std::string result = func_name_ + "(";
    bool first = true;
    for (const auto& arg : args_) {
        if (!first) result += ", ";
        result += arg->to_string();
        first = false;
    }
    result += ")";
    return result;
}

inline std::unique_ptr<Expression> FunctionExpr::clone() const {
    auto result = std::make_unique<FunctionExpr>(func_name_);
    for (const auto& arg : args_) {
        result->add_arg(arg->clone());
    }
    return result;
}

inline common::Value InExpr::evaluate() const {
    common::Value col_val = column_->evaluate();
    for (const auto& val : values_) {
        if (col_val == val->evaluate()) {
            return common::Value(!not_flag_);
        }
    }
    return common::Value(not_flag_);
}

inline std::string InExpr::to_string() const {
    std::string result = column_->to_string() + (not_flag_ ? " NOT IN (" : " IN (");
    bool first = true;
    for (const auto& val : values_) {
        if (!first) result += ", ";
        result += val->to_string();
        first = false;
    }
    result += ")";
    return result;
}

inline std::unique_ptr<Expression> InExpr::clone() const {
    std::vector<std::unique_ptr<Expression>> cloned_vals;
    for (const auto& val : values_) {
        cloned_vals.push_back(val->clone());
    }
    return std::make_unique<InExpr>(column_->clone(), std::move(cloned_vals), not_flag_);
}

inline common::Value IsNullExpr::evaluate() const {
    common::Value val = expr_->evaluate();
    bool result = val.is_null();
    return common::Value(not_flag_ ? !result : result);
}

inline std::string IsNullExpr::to_string() const {
    return expr_->to_string() + (not_flag_ ? " IS NOT NULL" : " IS NULL");
}

inline std::unique_ptr<Expression> IsNullExpr::clone() const {
    return std::make_unique<IsNullExpr>(expr_->clone(), not_flag_);
}

}  // namespace parser
}  // namespace cloudsql

#endif  // CLOUDSQL_PARSER_EXPRESSION_HPP
