/**
 * @file parser.cpp
 * @brief SQL Parser implementation
 *
 * @defgroup parser SQL Parser
 * @{
 */

#include "parser/parser.hpp"
#include <iostream>

namespace cloudsql {
namespace parser {

/**
 * @brief Construct a new Parser
 */
Parser::Parser(std::unique_ptr<Lexer> lexer) 
    : lexer_(std::move(lexer)) {}

/**
 * @brief Parse a single SQL statement
 */
std::unique_ptr<Statement> Parser::parse_statement() {
    Token tok = peek_token();
    
    std::unique_ptr<Statement> stmt = nullptr;
    switch (tok.type()) {
        case TokenType::Select:
            stmt = parse_select();
            break;
        case TokenType::Create:
            next_token(); // consume CREATE
            if (peek_token().type() == TokenType::Table) {
                stmt = parse_create_table();
            }
            break;
        case TokenType::Insert:
            stmt = parse_insert();
            break;
        case TokenType::Update:
            stmt = parse_update();
            break;
        case TokenType::Delete:
            stmt = parse_delete();
            break;
        case TokenType::Begin:
            next_token();
            stmt = std::make_unique<TransactionBeginStatement>();
            break;
        case TokenType::Commit:
            next_token();
            stmt = std::make_unique<TransactionCommitStatement>();
            break;
        case TokenType::Rollback:
            next_token();
            stmt = std::make_unique<TransactionRollbackStatement>();
            break;
        default:
            break;
    }

    return stmt;
}

/**
 * @brief Parse SELECT statement
 */
std::unique_ptr<Statement> Parser::parse_select() {
    auto stmt = std::make_unique<SelectStatement>();
    if (!consume(TokenType::Select)) return nullptr;
    
    /* DISTINCT */
    if (consume(TokenType::Distinct)) {
        stmt->set_distinct(true);
    }
    
    /* Columns */
    bool first = true;
    while (true) {
        if (!first && !consume(TokenType::Comma)) break;
        first = false;
        
        auto expr = parse_expression();
        if (!expr) {
            std::cerr << "Parser Error: Invalid column expression" << std::endl;
            return nullptr;
        }
        stmt->add_column(std::move(expr));
        
        if (peek_token().type() == TokenType::From) break;
    }
    
    /* FROM */
    if (consume(TokenType::From)) {
        auto from_expr = parse_expression();
        if (!from_expr) {
            std::cerr << "Parser Error: Invalid FROM expression" << std::endl;
            return nullptr;
        }
        stmt->add_from(std::move(from_expr));
    } else {
        std::cerr << "Parser Error: Missing FROM clause. Current token: " << peek_token().to_string() << std::endl;
        return nullptr;
    }
    
    /* WHERE */
    if (consume(TokenType::Where)) {
        auto where_expr = parse_expression();
        if (!where_expr) return nullptr;
        stmt->set_where(std::move(where_expr));
    }
    
    /* GROUP BY */
    if (peek_token().type() == TokenType::Group) {
        consume(TokenType::Group);
        if (consume(TokenType::By)) {
            bool g_first = true;
            while (true) {
                if (!g_first && !consume(TokenType::Comma)) break;
                g_first = false;
                
                auto expr = parse_expression();
                if (!expr) return nullptr;
                stmt->add_group_by(std::move(expr));
                
                if (peek_token().type() != TokenType::Comma) break;
            }
        } else {
            return nullptr;
        }
    }
    
    /* HAVING */
    if (peek_token().type() == TokenType::Having) {
        consume(TokenType::Having);
        auto having_expr = parse_expression();
        if (!having_expr) return nullptr;
        stmt->set_having(std::move(having_expr));
    }
    
    /* ORDER BY */
    if (peek_token().type() == TokenType::Order) {
        consume(TokenType::Order);
        if (consume(TokenType::By)) {
            bool o_first = true;
            while (true) {
                if (!o_first && !consume(TokenType::Comma)) break;
                o_first = false;
                
                auto expr = parse_expression();
                if (!expr) return nullptr;
                stmt->add_order_by(std::move(expr));
                
                if (peek_token().type() == TokenType::Asc || peek_token().type() == TokenType::Desc) {
                    next_token();
                }
                
                if (peek_token().type() != TokenType::Comma) break;
            }
        } else {
            return nullptr;
        }
    }
    
    /* LIMIT */
    if (consume(TokenType::Limit)) {
        Token val = next_token();
        if (val.type() == TokenType::Number) {
            stmt->set_limit(val.as_int64());
        } else {
            return nullptr;
        }
    }
    
    /* OFFSET */
    if (consume(TokenType::Offset)) {
        Token val = next_token();
        if (val.type() == TokenType::Number) {
            stmt->set_offset(val.as_int64());
        } else {
            return nullptr;
        }
    }
    
    return stmt;
}


/**
 * @brief Parse CREATE TABLE statement
 */
std::unique_ptr<Statement> Parser::parse_create_table() {
    auto stmt = std::make_unique<CreateTableStatement>();
    if (!consume(TokenType::Table)) return nullptr;
    
    /* IF NOT EXISTS */
    if (consume(TokenType::Not)) {
        if (!consume(TokenType::Exists)) return nullptr;
    }
    
    Token name = next_token();
    if (name.type() != TokenType::Identifier) return nullptr;
    stmt->set_table_name(name.lexeme());
    
    if (!consume(TokenType::LParen)) return nullptr;
    
    bool first = true;
    while (true) {
        if (!first && !consume(TokenType::Comma)) break;
        first = false;
        
        Token col_name = next_token();
        if (col_name.type() != TokenType::Identifier) return nullptr;
        
        Token col_type = next_token();
        std::string type_str = col_type.lexeme();
        
        if (col_type.type() == TokenType::Varchar) {
             if (consume(TokenType::LParen)) {
                 Token len = next_token();
                 if (len.type() != TokenType::Number) return nullptr;
                 consume(TokenType::RParen);
                 type_str += "(" + len.lexeme() + ")";
             }
        }
        
        stmt->add_column(col_name.lexeme(), type_str);
        
        while (true) {
            Token t = peek_token();
            if (t.type() == TokenType::Primary) {
                consume(TokenType::Primary);
                if (!consume(TokenType::Key)) return nullptr;
                stmt->get_last_column().is_primary_key_ = true;
            } else if (t.type() == TokenType::Not) {
                consume(TokenType::Not);
                if (!consume(TokenType::Null)) return nullptr;
                stmt->get_last_column().is_not_null_ = true;
            } else if (t.type() == TokenType::Unique) {
                consume(TokenType::Unique);
                stmt->get_last_column().is_unique_ = true;
            } else {
                break;
            }
        }
        
        if (peek_token().type() == TokenType::RParen) break;
    }
    
    if (!consume(TokenType::RParen)) return nullptr;
    return stmt;
}

/**
 * @brief Parse INSERT statement
 */
std::unique_ptr<Statement> Parser::parse_insert() {
    auto stmt = std::make_unique<InsertStatement>();
    if (!consume(TokenType::Insert)) return nullptr;
    if (!consume(TokenType::Into)) return nullptr;
    
    Token table_tok = next_token();
    if (table_tok.type() != TokenType::Identifier) return nullptr;
    stmt->set_table(std::make_unique<ColumnExpr>(table_tok.lexeme()));
    
    if (consume(TokenType::LParen)) {
        bool first = true;
        while (true) {
            if (!first && !consume(TokenType::Comma)) break;
            first = false;
            
            Token col_tok = next_token();
            if (col_tok.type() != TokenType::Identifier) return nullptr;
            stmt->add_column(std::make_unique<ColumnExpr>(col_tok.lexeme()));
            
            if (peek_token().type() == TokenType::RParen) break;
        }
        if (!consume(TokenType::RParen)) return nullptr;
    }
    
    if (!consume(TokenType::Values)) return nullptr;
    
    bool first_row = true;
    while (true) {
        if (!first_row && !consume(TokenType::Comma)) break;
        first_row = false;
        
        if (!consume(TokenType::LParen)) return nullptr;
        
        std::vector<std::unique_ptr<Expression>> row;
        bool first_val = true;
        while (true) {
            if (!first_val && !consume(TokenType::Comma)) break;
            first_val = false;
            
            auto expr = parse_expression();
            if (!expr) return nullptr;
            row.push_back(std::move(expr));
            
            if (peek_token().type() == TokenType::RParen) break;
        }
        stmt->add_row(std::move(row));
        if (!consume(TokenType::RParen)) return nullptr;
        
        if (peek_token().type() != TokenType::Comma) break;
    }
    
    return stmt;
}

/**
 * @brief Parse UPDATE statement
 */
std::unique_ptr<Statement> Parser::parse_update() {
    auto stmt = std::make_unique<UpdateStatement>();
    if (!consume(TokenType::Update)) return nullptr;

    Token table_tok = next_token();
    if (table_tok.type() != TokenType::Identifier) return nullptr;
    stmt->set_table(std::make_unique<ColumnExpr>(table_tok.lexeme()));

    if (!consume(TokenType::Set)) return nullptr;

    bool first = true;
    while (true) {
        if (!first && !consume(TokenType::Comma)) break;
        first = false;

        Token col_tok = next_token();
        if (col_tok.type() != TokenType::Identifier) return nullptr;

        if (!consume(TokenType::Eq)) return nullptr;

        auto val_expr = parse_expression();
        if (!val_expr) return nullptr;

        stmt->add_set(std::make_unique<ColumnExpr>(col_tok.lexeme()), std::move(val_expr));

        if (peek_token().type() != TokenType::Comma) break;
    }

    if (consume(TokenType::Where)) {
        auto where_expr = parse_expression();
        if (!where_expr) return nullptr;
        stmt->set_where(std::move(where_expr));
    }

    return stmt;
}

/**
 * @brief Parse DELETE statement
 */
std::unique_ptr<Statement> Parser::parse_delete() {
    auto stmt = std::make_unique<DeleteStatement>();
    if (!consume(TokenType::Delete)) return nullptr;
    if (!consume(TokenType::From)) return nullptr;

    Token table_tok = next_token();
    if (table_tok.type() != TokenType::Identifier) return nullptr;
    stmt->set_table(std::make_unique<ColumnExpr>(table_tok.lexeme()));

    if (consume(TokenType::Where)) {
        auto where_expr = parse_expression();
        if (!where_expr) return nullptr;
        stmt->set_where(std::move(where_expr));
    }

    return stmt;
}

/**
 * @brief Parse expression (Precedence Climbing)
 */
std::unique_ptr<Expression> Parser::parse_expression() {
    return parse_or();
}

std::unique_ptr<Expression> Parser::parse_or() {
    auto left = parse_and();
    if (!left) return nullptr;
    while (peek_token().type() == TokenType::Or) {
        Token op = next_token();
        auto right = parse_and();
        if (!right) return nullptr;
        left = std::make_unique<BinaryExpr>(std::move(left), op.type(), std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::parse_and() {
    auto left = parse_not();
    if (!left) return nullptr;
    while (peek_token().type() == TokenType::And) {
        consume(TokenType::And);
        auto right = parse_not();
        if (!right) return nullptr;
        left = std::make_unique<BinaryExpr>(std::move(left), TokenType::And, std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::parse_not() {
    if (peek_token().type() == TokenType::Not) {
        consume(TokenType::Not);
        auto inner = parse_not();
        if (!inner) return nullptr;
        return std::make_unique<UnaryExpr>(TokenType::Not, std::move(inner));
    }
    return parse_compare();
}

std::unique_ptr<Expression> Parser::parse_compare() {
    auto left = parse_add_sub();
    if (!left) return nullptr;
    
    Token tok = peek_token();
    if (tok.type() == TokenType::Eq || tok.type() == TokenType::Ne ||
        tok.type() == TokenType::Lt || tok.type() == TokenType::Le ||
        tok.type() == TokenType::Gt || tok.type() == TokenType::Ge) {
        next_token();
        auto right = parse_add_sub();
        if (!right) return nullptr;
        return std::make_unique<BinaryExpr>(std::move(left), tok.type(), std::move(right));
    }
    
    return left;
}

std::unique_ptr<Expression> Parser::parse_add_sub() {
    auto left = parse_mul_div();
    if (!left) return nullptr;
    while (peek_token().type() == TokenType::Plus || peek_token().type() == TokenType::Minus) {
        Token op = next_token();
        auto right = parse_mul_div();
        if (!right) return nullptr;
        left = std::make_unique<BinaryExpr>(std::move(left), op.type(), std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::parse_mul_div() {
    auto left = parse_unary();
    if (!left) return nullptr;
    while (peek_token().type() == TokenType::Star || peek_token().type() == TokenType::Slash) {
        Token op = next_token();
        auto right = parse_unary();
        if (!right) return nullptr;
        left = std::make_unique<BinaryExpr>(std::move(left), op.type(), std::move(right));
    }
    return left;
}

std::unique_ptr<Expression> Parser::parse_unary() {
    if (peek_token().type() == TokenType::Minus || peek_token().type() == TokenType::Plus) {
        Token op = next_token();
        auto inner = parse_unary();
        if (!inner) return nullptr;
        return std::make_unique<UnaryExpr>(op.type(), std::move(inner));
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
    } 
    else if (tok.type() == TokenType::String) {
        next_token();
        return std::make_unique<ConstantExpr>(common::Value::make_text(tok.as_string()));
    } 
    else if (tok.type() == TokenType::Identifier || tok.is_keyword()) {
        Token id = next_token();
        /* Check if it's a function call */
        if (peek_token().type() == TokenType::LParen) {
            consume(TokenType::LParen);
            
            /* Normalize function name to uppercase for consistency */
            std::string func_name = id.lexeme();
            std::transform(func_name.begin(), func_name.end(), func_name.begin(), ::toupper);
            
            auto func = std::make_unique<FunctionExpr>(func_name);
            
            /* Handle DISTINCT inside function call, e.g. COUNT(DISTINCT col) */
            if (peek_token().type() == TokenType::Distinct) {
                consume(TokenType::Distinct);
                func->set_distinct(true);
            }

            bool first = true;
            while (peek_token().type() != TokenType::RParen) {
                if (!first && !consume(TokenType::Comma)) break;
                first = false;
                auto arg = parse_expression();
                if (!arg) {
                    std::cerr << "DEBUG: Failed to parse function arg for " << func_name << std::endl;
                    return nullptr;
                }
                func->add_arg(std::move(arg));
            }
            if (!consume(TokenType::RParen)) {
                std::cerr << "DEBUG: Missing RParen for function " << func_name << std::endl;
                return nullptr;
            }
            return func;
        }
        return std::make_unique<ColumnExpr>(id.lexeme());
    } 
    else if (consume(TokenType::LParen)) {
        auto expr = parse_expression();
        if (!expr) {
            std::cerr << "DEBUG: Failed to parse expression inside LParen" << std::endl;
            return nullptr;
        }
        if (!consume(TokenType::RParen)) {
            std::cerr << "DEBUG: Missing RParen after expression" << std::endl;
            return nullptr;
        }
        return expr;
    }
    
    std::cerr << "DEBUG: Unrecognized primary token: " << tok.to_string() << std::endl;
    return nullptr;
}

/**
 * @brief Get next token from lexer
 */
Token Parser::next_token() {
    if (has_current_) {
        has_current_ = false;
        return current_token_;
    }
    return lexer_->next_token();
}

/**
 * @brief Peek at current token without consuming it
 */
Token Parser::peek_token() {
    if (!has_current_) {
        current_token_ = lexer_->next_token();
        has_current_ = true;
    }
    return current_token_;
}

/**
 * @brief Consume token if it matches expected type
 */
bool Parser::consume(TokenType type) {
    if (peek_token().type() == type) {
        next_token();
        return true;
    }
    return false;
}

} // namespace parser
} // namespace cloudsql

/** @} */ /* parser */
