/**
 * @file token.hpp
 * @brief Token class for SQL Lexer
 */

#ifndef CLOUDSQL_PARSER_TOKEN_HPP
#define CLOUDSQL_PARSER_TOKEN_HPP

#include <cstdint>
#include <string>

namespace cloudsql {
namespace parser {

/**
 * @brief Token types for SQL
 */
enum class TokenType {
    End = 0,
    
    // Keywords
    Select, From, Where, Insert, Into, Values,
    Update, Set, Delete, Create, Table, Drop,
    Index, On, And, Or, Not, In, Like, Is,
    Null, Primary, Key, Foreign, References,
    Join, Left, Right, Inner, Outer,
    Order, By, Asc, Desc, Group, Having,
    Limit, Offset, As, Distinct,
    Count, Sum, Avg, Min, Max,
    Begin, Commit, Rollback,
    Truncate, Alter, Add, Column, Type,
    Constraint, Unique, Check, Default, Exists,
    Varchar,
    
    // Identifiers and literals
    Identifier, String, Number, Param,
    
    // Operators
    Eq, Ne, Lt, Le, Gt, Ge,
    Plus, Minus, Star, Slash, Percent, Concat,
    
    // Delimiters
    LParen, RParen, Comma, Semicolon, Dot, Colon,
    
    // Error
    Error
};

/**
 * @brief Token class with type-safe storage
 */
class Token {
private:
    TokenType type_;
    std::string lexeme_;
    uint32_t line_;
    uint32_t column_;
    
    // Type-safe value storage
    bool bool_value_;
    int64_t int64_value_;
    double double_value_;
    std::string* string_value_;

public:
    Token();
    explicit Token(TokenType type);
    Token(TokenType type, const std::string& lexeme);
    Token(TokenType type, const std::string& lexeme, uint32_t line, uint32_t column);
    Token(TokenType type, int64_t value);
    Token(TokenType type, double value);
    Token(TokenType type, const std::string& value, bool is_string);
    
    ~Token();
    Token(const Token& other);
    Token(Token&& other) noexcept;
    Token& operator=(const Token& other);
    Token& operator=(Token&& other) noexcept;

    // Accessors
    TokenType type() const { return type_; }
    const std::string& lexeme() const { return lexeme_; }
    uint32_t line() const { return line_; }
    uint32_t column() const { return column_; }
    
    void set_type(TokenType type) { type_ = type; }
    void set_position(uint32_t line, uint32_t column) { line_ = line; column_ = column; }
    
    // Value accessors
    bool as_bool() const { return bool_value_; }
    int64_t as_int64() const { return int64_value_; }
    double as_double() const { return double_value_; }
    const std::string& as_string() const { 
        static std::string empty;
        return string_value_ ? *string_value_ : empty; 
    }
    
    // Type queries
    bool is_keyword() const;
    bool is_literal() const;
    bool is_operator() const;
    bool is_identifier() const;
    
    std::string to_string() const;
};

inline Token::Token()
    : type_(TokenType::End), lexeme_(""), line_(0), column_(0),
      bool_value_(false), int64_value_(0), double_value_(0.0), string_value_(nullptr) {}

inline Token::Token(TokenType type)
    : type_(type), lexeme_(""), line_(0), column_(0),
      bool_value_(false), int64_value_(0), double_value_(0.0), string_value_(nullptr) {}

inline Token::Token(TokenType type, const std::string& lexeme)
    : type_(type), lexeme_(lexeme), line_(0), column_(0),
      bool_value_(false), int64_value_(0), double_value_(0.0), string_value_(nullptr) {}

inline Token::Token(TokenType type, const std::string& lexeme, uint32_t line, uint32_t column)
    : type_(type), lexeme_(lexeme), line_(line), column_(column),
      bool_value_(false), int64_value_(0), double_value_(0.0), string_value_(nullptr) {}

inline Token::Token(TokenType type, int64_t value)
    : type_(type), lexeme_(std::to_string(value)), line_(0), column_(0),
      bool_value_(false), int64_value_(value), double_value_(0.0), string_value_(nullptr) {}

inline Token::Token(TokenType type, double value)
    : type_(type), lexeme_(std::to_string(value)), line_(0), column_(0),
      bool_value_(false), int64_value_(0), double_value_(value), string_value_(nullptr) {}

inline Token::Token(TokenType type, const std::string& value, bool is_string)
    : type_(type), lexeme_(is_string ? "'" + value + "'" : value), line_(0), column_(0),
      bool_value_(false), int64_value_(0), double_value_(0.0), 
      string_value_(is_string ? new std::string(value) : nullptr) {}

inline Token::~Token() {
    delete string_value_;
}

inline Token::Token(const Token& other)
    : type_(other.type_), lexeme_(other.lexeme_), line_(other.line_), column_(other.column_),
      bool_value_(other.bool_value_), int64_value_(other.int64_value_), double_value_(other.double_value_),
      string_value_(other.string_value_ ? new std::string(*other.string_value_) : nullptr) {}

inline Token::Token(Token&& other) noexcept
    : type_(other.type_), lexeme_(std::move(other.lexeme_)), line_(other.line_), column_(other.column_),
      bool_value_(other.bool_value_), int64_value_(other.int64_value_), double_value_(other.double_value_),
      string_value_(other.string_value_) {
    other.string_value_ = nullptr;
}

inline Token& Token::operator=(const Token& other) {
    if (this != &other) {
        Token temp(other);
        *this = std::move(temp);
    }
    return *this;
}

inline Token& Token::operator=(Token&& other) noexcept {
    if (this != &other) {
        delete string_value_;
        
        type_ = other.type_;
        lexeme_ = std::move(other.lexeme_);
        line_ = other.line_;
        column_ = other.column_;
        bool_value_ = other.bool_value_;
        int64_value_ = other.int64_value_;
        double_value_ = other.double_value_;
        string_value_ = other.string_value_;
        
        other.string_value_ = nullptr;
    }
    return *this;
}

inline bool Token::is_keyword() const {
    return type_ >= TokenType::Select && type_ <= TokenType::Varchar;
}

inline bool Token::is_literal() const {
    return type_ == TokenType::String || type_ == TokenType::Number || type_ == TokenType::Param;
}

inline bool Token::is_operator() const {
    return type_ >= TokenType::Eq && type_ <= TokenType::Concat;
}

inline bool Token::is_identifier() const {
    return type_ == TokenType::Identifier;
}

inline std::string Token::to_string() const {
    return "Token(type=" + std::to_string(static_cast<int>(type_)) + ", lexeme='" + lexeme_ + "')";
}

}  // namespace parser
}  // namespace cloudsql

#endif  // CLOUDSQL_PARSER_TOKEN_HPP
