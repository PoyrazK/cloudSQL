/**
 * @file lexer.hpp
 * @brief SQL Lexer class for tokenizing SQL statements
 */

#ifndef CLOUDSQL_PARSER_LEXER_HPP
#define CLOUDSQL_PARSER_LEXER_HPP

#include <cstdint>
#include <string>
#include <map>
#include <vector>
#include "parser/token.hpp"

namespace cloudsql {
namespace parser {

/**
 * @brief SQL Lexer class
 *
 * Converts SQL text into a stream of tokens.
 */
class Lexer {
private:
    std::string input_;
    size_t position_;
    uint32_t line_;
    uint32_t column_;
    char current_char_;
    
    // Keyword lookup table
    static const std::map<std::string, TokenType> keywords_;
    
    // Initialize keywords
    static std::map<std::string, TokenType> init_keywords();

public:
    /**
     * @brief Construct a lexer
     * @param input SQL input string
     */
    explicit Lexer(const std::string& input);
    
    /**
     * @brief Get the next token
     * @return Next token in the stream
     */
    Token next_token();
    
    /**
     * @brief Peek at the next token without consuming it
     * @return Next token (or End token if at end)
     */
    Token peek_token();
    
    /**
     * @brief Check if at end of input
     * @return true if at end
     */
    bool is_at_end() const;
    
    /**
     * @brief Get current line number
     * @return Current line (1-indexed)
     */
    uint32_t line() const { return line_; }
    
    /**
     * @brief Get current column number
     * @return Current column (1-indexed)
     */
    uint32_t column() const { return column_; }

private:
    /**
     * @brief Advance to the next character
     */
    void advance();
    
    /**
     * @brief Skip whitespace characters
     */
    void skip_whitespace();
    
    /**
     * @brief Skip a comment
     */
    void skip_comment();
    
    /**
     * @brief Check if current character is a letter
     */
    bool is_letter() const;
    
    /**
     * @brief Check if current character is a digit
     */
    bool is_digit() const;
    
    /**
     * @brief Check if current character is part of an identifier
     */
    bool is_identifier_char() const;
    
    /**
     * @brief Read a number literal
     * @return Number token
     */
    Token read_number();
    
    /**
     * @brief Read a string literal
     * @return String token
     */
    Token read_string();
    
    /**
     * @brief Read an identifier or keyword
     * @return Identifier or keyword token
     */
    Token read_identifier();
    
    /**
     * @brief Read an operator
     * @return Operator token
     */
    Token read_operator();
    
    /**
     * @brief Create a token with current position
     * @param type Token type
     * @return Token with position set
     */
    Token make_token(TokenType type);
    
    /**
     * @brief Create an error token
     * @param message Error message
     * @return Error token
     */
    Token make_error(const std::string& message);
};

// Static keyword initialization
inline std::map<std::string, TokenType> Lexer::init_keywords() {
    return {
        {"SELECT", TokenType::Select},
        {"FROM", TokenType::From},
        {"WHERE", TokenType::Where},
        {"INSERT", TokenType::Insert},
        {"INTO", TokenType::Into},
        {"VALUES", TokenType::Values},
        {"UPDATE", TokenType::Update},
        {"SET", TokenType::Set},
        {"DELETE", TokenType::Delete},
        {"CREATE", TokenType::Create},
        {"TABLE", TokenType::Table},
        {"DROP", TokenType::Drop},
        {"INDEX", TokenType::Index},
        {"ON", TokenType::On},
        {"AND", TokenType::And},
        {"OR", TokenType::Or},
        {"NOT", TokenType::Not},
        {"IN", TokenType::In},
        {"LIKE", TokenType::Like},
        {"IS", TokenType::Is},
        {"NULL", TokenType::Null},
        {"PRIMARY", TokenType::Primary},
        {"KEY", TokenType::Key},
        {"FOREIGN", TokenType::Foreign},
        {"REFERENCES", TokenType::References},
        {"JOIN", TokenType::Join},
        {"LEFT", TokenType::Left},
        {"RIGHT", TokenType::Right},
        {"INNER", TokenType::Inner},
        {"OUTER", TokenType::Outer},
        {"ORDER", TokenType::Order},
        {"BY", TokenType::By},
        {"ASC", TokenType::Asc},
        {"DESC", TokenType::Desc},
        {"GROUP", TokenType::Group},
        {"HAVING", TokenType::Having},
        {"LIMIT", TokenType::Limit},
        {"OFFSET", TokenType::Offset},
        {"AS", TokenType::As},
        {"DISTINCT", TokenType::Distinct},
        {"COUNT", TokenType::Count},
        {"SUM", TokenType::Sum},
        {"AVG", TokenType::Avg},
        {"MIN", TokenType::Min},
        {"MAX", TokenType::Max},
        {"BEGIN", TokenType::Begin},
        {"COMMIT", TokenType::Commit},
        {"ROLLBACK", TokenType::Rollback},
        {"TRUNCATE", TokenType::Truncate},
        {"ALTER", TokenType::Alter},
        {"ADD", TokenType::Add},
        {"COLUMN", TokenType::Column},
        {"TYPE", TokenType::Type},
        {"CONSTRAINT", TokenType::Constraint},
        {"UNIQUE", TokenType::Unique},
        {"CHECK", TokenType::Check},
        {"DEFAULT", TokenType::Default},
        {"EXISTS", TokenType::Exists},
        {"VARCHAR", TokenType::Varchar}
    };
}

inline const std::map<std::string, TokenType> Lexer::keywords_ = Lexer::init_keywords();

inline Lexer::Lexer(const std::string& input)
    : input_(input), position_(0), line_(1), column_(1) {
    if (!input_.empty()) {
        current_char_ = input_[0];
    } else {
        current_char_ = '\0';
    }
}

inline void Lexer::advance() {
    if (position_ < input_.size()) {
        if (current_char_ == '\n') {
            line_++;
            column_ = 1;
        } else {
            column_++;
        }
        position_++;
        if (position_ < input_.size()) {
            current_char_ = input_[position_];
        } else {
            current_char_ = '\0';
        }
    }
}

inline void Lexer::skip_whitespace() {
    while (!is_at_end()) {
        if (current_char_ == ' ' || current_char_ == '\t' || 
            current_char_ == '\n' || current_char_ == '\r') {
            advance();
        } else if (current_char_ == '-') {
            // Check for comment
            if (position_ + 1 < input_.size() && input_[position_ + 1] == '-') {
                skip_comment();
            } else {
                break;
            }
        } else {
            break;
        }
    }
}

inline void Lexer::skip_comment() {
    while (!is_at_end() && current_char_ != '\n') {
        advance();
    }
}

inline bool Lexer::is_letter() const {
    return (current_char_ >= 'a' && current_char_ <= 'z') ||
           (current_char_ >= 'A' && current_char_ <= 'Z') ||
           current_char_ == '_';
}

inline bool Lexer::is_digit() const {
    return current_char_ >= '0' && current_char_ <= '9';
}

inline bool Lexer::is_identifier_char() const {
    return is_letter() || is_digit();
}

inline bool Lexer::is_at_end() const {
    return position_ >= input_.size();
}

inline Token Lexer::make_token(TokenType type) {
    return Token(type, "", line_, column_);
}

inline Token Lexer::make_error(const std::string& message) {
    Token error(TokenType::Error, message);
    error.set_position(line_, column_);
    return error;
}

inline Token Lexer::read_number() {
    uint32_t start_line = line_;
    uint32_t start_col = column_;
    
    std::string number_str;
    while (is_digit()) {
        number_str += current_char_;
        advance();
    }
    
    // Check for decimal point
    if (current_char_ == '.' && position_ + 1 < input_.size() && is_digit()) {
        number_str += current_char_;
        advance();
        while (is_digit()) {
            number_str += current_char_;
            advance();
        }
        
        // Check for exponent
        if (current_char_ == 'e' || current_char_ == 'E') {
            number_str += current_char_;
            advance();
            if (current_char_ == '+' || current_char_ == '-') {
                number_str += current_char_;
                advance();
            }
            while (is_digit()) {
                number_str += current_char_;
                advance();
            }
        }
        
        double value = std::stod(number_str);
        Token tok(TokenType::Number, value);
        tok.set_position(start_line, start_col);
        return tok;
    }
    
    int64_t value = std::stoll(number_str);
    Token tok(TokenType::Number, value);
    tok.set_position(start_line, start_col);
    return tok;
}

inline Token Lexer::read_string() {
    uint32_t start_line = line_;
    uint32_t start_col = column_;
    
    char quote_char = current_char_;
    advance(); // Skip opening quote
    
    std::string value;
    while (!is_at_end() && current_char_ != quote_char) {
        if (current_char_ == '\\' && position_ + 1 < input_.size()) {
            advance();
            switch (current_char_) {
                case 'n': value += '\n'; break;
                case 't': value += '\t'; break;
                case 'r': value += '\r'; break;
                case '\'': value += '\''; break;
                case '"': value += '"'; break;
                case '\\': value += '\\'; break;
                default: value += current_char_; break;
            }
        } else {
            value += current_char_;
        }
        advance();
    }
    
    // Skip closing quote
    if (!is_at_end() && current_char_ == quote_char) {
        advance();
    }
    
    Token tok(TokenType::String, value, true);
    tok.set_position(start_line, start_col);
    return tok;
}

inline Token Lexer::read_identifier() {
    uint32_t start_line = line_;
    uint32_t start_col = column_;
    
    std::string identifier;
    while (is_identifier_char()) {
        identifier += current_char_;
        advance();
    }
    
    // Check if it's a keyword
    auto it = keywords_.find(identifier);
    if (it != keywords_.end()) {
        Token tok(it->second, identifier);
        tok.set_position(start_line, start_col);
        return tok;
    }
    
    // It's an identifier
    Token tok(TokenType::Identifier, identifier);
    tok.set_position(start_line, start_col);
    return tok;
}

inline Token Lexer::read_operator() {
    uint32_t start_line = line_;
    uint32_t start_col = column_;
    
    char c = current_char_;
    advance();
    
    // Two-character operators
    if (position_ < input_.size()) {
        char next = current_char_;
        
        if (c == '=' && next == '=') {
            advance();
            Token tok(TokenType::Eq, "==");
            tok.set_position(start_line, start_col);
            return tok;
        }
        if (c == '<' && next == '>') {
            advance();
            Token tok(TokenType::Ne, "<>");
            tok.set_position(start_line, start_col);
            return tok;
        }
        if (c == '<' && next == '=') {
            advance();
            Token tok(TokenType::Le, "<=");
            tok.set_position(start_line, start_col);
            return tok;
        }
        if (c == '>' && next == '=') {
            advance();
            Token tok(TokenType::Ge, ">=");
            tok.set_position(start_line, start_col);
            return tok;
        }
        if (c == '|' && next == '|') {
            advance();
            Token tok(TokenType::Concat, "||");
            tok.set_position(start_line, start_col);
            return tok;
        }
    }
    
    // Single-character operators
    std::string op(1, c);
    TokenType type;
    
    switch (c) {
        case '=': type = TokenType::Eq; break;
        case '<': type = TokenType::Lt; break;
        case '>': type = TokenType::Gt; break;
        case '+': type = TokenType::Plus; break;
        case '-': type = TokenType::Minus; break;
        case '*': type = TokenType::Star; break;
        case '/': type = TokenType::Slash; break;
        case '%': type = TokenType::Percent; break;
        case '(': type = TokenType::LParen; break;
        case ')': type = TokenType::RParen; break;
        case ',': type = TokenType::Comma; break;
        case ';': type = TokenType::Semicolon; break;
        case '.': type = TokenType::Dot; break;
        case ':': type = TokenType::Colon; break;
        default:
            return make_error("Unknown operator: " + op);
    }
    
    Token tok(type, op);
    tok.set_position(start_line, start_col);
    return tok;
}

inline Token Lexer::next_token() {
    skip_whitespace();
    
    if (is_at_end()) {
        return Token(TokenType::End, "", line_, column_);
    }
    
    char c = current_char_;
    
    // Number
    if (is_digit()) {
        return read_number();
    }
    
    // String
    if (c == '\'' || c == '"') {
        return read_string();
    }
    
    // Identifier or keyword
    if (is_letter()) {
        return read_identifier();
    }
    
    // Operator
    return read_operator();
}

inline Token Lexer::peek_token() {
    if (is_at_end()) {
        return Token(TokenType::End, "", line_, column_);
    }
    
    // Save state
    size_t saved_pos = position_;
    uint32_t saved_line = line_;
    uint32_t saved_col = column_;
    char saved_char = current_char_;
    
    // Get next token
    Token tok = next_token();
    
    // Restore state
    position_ = saved_pos;
    line_ = saved_line;
    column_ = saved_col;
    current_char_ = saved_char;
    
    return tok;
}

}  // namespace parser
}  // namespace cloudsql

#endif  // CLOUDSQL_PARSER_LEXER_HPP
