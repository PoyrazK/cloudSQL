#include "parser/parser.hpp"
#include <iostream>

namespace cloudsql {
namespace parser {

Parser::Parser(std::unique_ptr<Lexer> lexer) 
    : lexer_(std::move(lexer)) {}

std::unique_ptr<Statement> Parser::parse_statement() {
    Token tok = peek_token();
    
    switch (tok.type()) {
        case TokenType::Select:
            return parse_select();
        case TokenType::Create:
            // Need to peek ahead to see if it's CREATE TABLE
            next_token(); // consume CREATE
            if (peek_token().type() == TokenType::Table) {
                return parse_create_table();
            }
            break;
        case TokenType::Insert:
            return parse_insert();
        // TODO: Other statements
        default:
            break;
    }
    return nullptr;
}

std::unique_ptr<Statement> Parser::parse_select() {
    auto stmt = std::make_unique<SelectStatement>();
    consume(TokenType::Select);
    
    if (peek_token().type() == TokenType::Distinct) {
        consume(TokenType::Distinct);
        stmt->set_distinct(true);
    }
    
    // Parse columns
    bool first = true;
    while (first || consume(TokenType::Comma)) {
        first = false;
        stmt->add_column(parse_expression());
    }
    
    // FROM
    if (consume(TokenType::From)) {
        stmt->add_from(parse_expression()); // Simplified: treating table as expression (ColumnExpr)
    }
    
    // WHERE
    if (consume(TokenType::Where)) {
        stmt->set_where(parse_expression());
    }
    
    // GROUP BY
    if (consume(TokenType::Group) && consume(TokenType::By)) {
        bool g_first = true;
        while (g_first || consume(TokenType::Comma)) {
            g_first = false;
            stmt->add_group_by(parse_expression());
        }
    }
    
    // HAVING
    if (consume(TokenType::Having)) {
        stmt->set_having(parse_expression());
    }
    
    // ORDER BY
    if (consume(TokenType::Order) && consume(TokenType::By)) {
        bool o_first = true;
        while (o_first || consume(TokenType::Comma)) {
            o_first = false;
            stmt->add_order_by(parse_expression());
        }
    }
    
    // LIMIT
    if (consume(TokenType::Limit)) {
        Token val = next_token();
        if (val.type() == TokenType::Number) {
            stmt->set_limit(val.as_int64());
        }
    }
    
    // OFFSET
    if (consume(TokenType::Offset)) {
        Token val = next_token();
        if (val.type() == TokenType::Number) {
            stmt->set_offset(val.as_int64());
        }
    }
    
    return stmt;
}

std::unique_ptr<Statement> Parser::parse_create_table() {
    auto stmt = std::make_unique<CreateTableStatement>();
    consume(TokenType::Table); // CREATE already consumed
    
    // IF NOT EXISTS (simplified parsing)
    if (peek_token().type() == TokenType::Not) {
        consume(TokenType::Not);
        consume(TokenType::Exists);
        // stmt->set_if_not_exists(true); 
    }
    
    Token name = next_token();
    if (name.type() != TokenType::Identifier) {
        return nullptr;
    }
    stmt->set_table_name(name.lexeme());
    
    consume(TokenType::LParen);
    
    bool first = true;
    while (first || consume(TokenType::Comma)) {
        first = false;
        Token col_name = next_token();
        Token col_type = next_token();
        
        if (col_name.type() != TokenType::Identifier) break;
        
        std::string type_str = col_type.lexeme();
        // Handle VARCHAR(N)
        if (col_type.type() == TokenType::Varchar) {
             if (consume(TokenType::LParen)) {
                 Token len = next_token();
                 consume(TokenType::RParen);
                 type_str += "(" + len.lexeme() + ")";
             }
        }
        
        stmt->add_column(col_name.lexeme(), type_str);
        
        // Constraints
        while (true) {
            if (peek_token().type() == TokenType::Primary) {
                consume(TokenType::Primary);
                consume(TokenType::Key);
                // stmt->get_last_column().is_primary_key_ = true;
            } else if (peek_token().type() == TokenType::Not) {
                consume(TokenType::Not);
                consume(TokenType::Null);
                // stmt->get_last_column().is_not_null_ = true;
            } else {
                break;
            }
        }
        
        if (peek_token().type() == TokenType::RParen) break;
    }
    
    consume(TokenType::RParen);
    return stmt;
}

std::unique_ptr<Statement> Parser::parse_insert() {
    // TODO: Implement
    return nullptr;
}

// Expression Parsing (Precedence Climbing)

std::unique_ptr<Expression> Parser::parse_expression() {
    return parse_or();
}

std::unique_ptr<Expression> Parser::parse_or() {
    auto left = parse_and();
    while (consume(TokenType::Or)) {
        auto right = parse_and();
        left = std::make_unique<BinaryExpr>(std::move(left), TokenType::Or, std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::parse_and() {
    auto left = parse_not();
    while (consume(TokenType::And)) {
        auto right = parse_not();
        left = std::make_unique<BinaryExpr>(std::move(left), TokenType::And, std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::parse_not() {
    if (consume(TokenType::Not)) {
        return std::make_unique<UnaryExpr>(TokenType::Not, parse_not());
    }
    return parse_compare();
}

std::unique_ptr<Expression> Parser::parse_compare() {
    auto left = parse_add_sub();
    
    Token tok = peek_token();
    if (tok.type() == TokenType::Eq || tok.type() == TokenType::Ne ||
        tok.type() == TokenType::Lt || tok.type() == TokenType::Le ||
        tok.type() == TokenType::Gt || tok.type() == TokenType::Ge) {
        next_token();
        auto right = parse_add_sub();
        return std::make_unique<BinaryExpr>(std::move(left), tok.type(), std::move(right));
    }
    
    return left;
}

std::unique_ptr<Expression> Parser::parse_add_sub() {
    auto left = parse_mul_div();
    while (peek_token().type() == TokenType::Plus || peek_token().type() == TokenType::Minus) {
        Token op = next_token();
        auto right = parse_mul_div();
        left = std::make_unique<BinaryExpr>(std::move(left), op.type(), std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::parse_mul_div() {
    auto left = parse_unary();
    while (peek_token().type() == TokenType::Star || peek_token().type() == TokenType::Slash) {
        Token op = next_token();
        auto right = parse_unary();
        left = std::make_unique<BinaryExpr>(std::move(left), op.type(), std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::parse_unary() {
    if (peek_token().type() == TokenType::Minus || peek_token().type() == TokenType::Plus) {
        Token op = next_token();
        return std::make_unique<UnaryExpr>(op.type(), parse_unary());
    }
    return parse_primary();
}

std::unique_ptr<Expression> Parser::parse_primary() {
    Token tok = peek_token();
    
    if (tok.type() == TokenType::Number) {
        next_token();
        if (tok.lexeme().find('.') != std::string::npos) {
            return std::make_unique<ConstantExpr>(common::Value::make_float64(tok.as_double()));
        } else {
            return std::make_unique<ConstantExpr>(common::Value::make_int64(tok.as_int64()));
        }
    } else if (tok.type() == TokenType::String) {
        next_token();
        return std::make_unique<ConstantExpr>(common::Value::make_text(tok.as_string()));
    } else if (tok.type() == TokenType::Identifier) {
        next_token();
        return std::make_unique<ColumnExpr>(tok.lexeme());
    } else if (consume(TokenType::LParen)) {
        auto expr = parse_expression();
        consume(TokenType::RParen);
        return expr;
    }
    
    return nullptr;
}

// Helpers

Token Parser::next_token() {
    if (has_current_) {
        has_current_ = false;
        return current_token_;
    }
    return lexer_->next_token();
}

Token Parser::peek_token() {
    if (!has_current_) {
        current_token_ = lexer_->next_token();
        has_current_ = true;
    }
    return current_token_;
}

bool Parser::consume(TokenType type) {
    if (peek_token().type() == type) {
        next_token();
        return true;
    }
    return false;
}

} // namespace parser
} // namespace cloudsql
