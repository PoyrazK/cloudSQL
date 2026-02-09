/**
 * @file lexer.h
 * @brief SQL Lexer for tokenizing SQL statements
 *
 * @defgroup lexer Lexer
 * @{
 */

#ifndef SQL_ENGINE_PARSER_LEXER_H
#define SQL_ENGINE_PARSER_LEXER_H

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/* Token types */
typedef enum {
    TOKEN_EOF = 0,
    
    /* Keywords */
    TOKEN_SELECT,
    TOKEN_FROM,
    TOKEN_WHERE,
    TOKEN_INSERT,
    TOKEN_INTO,
    TOKEN_VALUES,
    TOKEN_UPDATE,
    TOKEN_SET,
    TOKEN_DELETE,
    TOKEN_CREATE,
    TOKEN_TABLE,
    TOKEN_DROP,
    TOKEN_INDEX,
    TOKEN_ON,
    TOKEN_AND,
    TOKEN_OR,
    TOKEN_NOT,
    TOKEN_IN,
    TOKEN_LIKE,
    TOKEN_IS,
    TOKEN_NULL,
    TOKEN_PRIMARY,
    TOKEN_KEY,
    TOKEN_FOREIGN,
    TOKEN_REFERENCES,
    TOKEN_JOIN,
    TOKEN_LEFT,
    TOKEN_RIGHT,
    TOKEN_INNER,
    TOKEN_OUTER,
    TOKEN_ORDER,
    TOKEN_BY,
    TOKEN_ASC,
    TOKEN_DESC,
    TOKEN_GROUP,
    TOKEN_HAVING,
    TOKEN_LIMIT,
    TOKEN_OFFSET,
    TOKEN_AS,
    TOKEN_DISTINCT,
    TOKEN_COUNT,
    TOKEN_SUM,
    TOKEN_AVG,
    TOKEN_MIN,
    TOKEN_MAX,
    TOKEN_BEGIN,
    TOKEN_COMMIT,
    TOKEN_ROLLBACK,
    TOKEN_TRUNCATE,
    TOKEN_ALTER,
    TOKEN_ADD,
    TOKEN_COLUMN,
    TOKEN_TYPE,
    TOKEN_CONSTRAINT,
    TOKEN_UNIQUE,
    TOKEN_CHECK,
    TOKEN_DEFAULT,
    
    /* Identifiers and literals */
    TOKEN_IDENTIFIER,
    TOKEN_STRING,
    TOKEN_NUMBER,
    TOKEN_PARAM,
    
    /* Operators */
    TOKEN_EQ,            /* = */
    TOKEN_NE,           /* <> or != */
    TOKEN_LT,           /* < */
    TOKEN_LE,           /* <= */
    TOKEN_GT,           /* > */
    TOKEN_GE,           /* >= */
    TOKEN_PLUS,         /* + */
    TOKEN_MINUS,        /* - */
    TOKEN_STAR,         /* * */
    TOKEN_SLASH,        /* / */
    TOKEN_PERCENT,      /* % */
    TOKEN_CONCAT,       /* || */
    
    /* Delimiters */
    TOKEN_LPAREN,       /* ( */
    TOKEN_RPAREN,       /* ) */
    TOKEN_COMMA,        /* , */
    TOKEN_SEMICOLON,    /* ; */
    TOKEN_DOT,          /* . */
    TOKEN_COLON,        /* : */
    
    /* Error */
    TOKEN_ERROR
} token_type_t;

/**
 * @brief Token structure
 */
typedef struct {
    token_type_t type;
    char *lexeme;
    size_t length;
    uint32_t line;
    uint32_t column;
    union {
        int64_t int_val;
        double float_val;
        char *str_val;
    } value;
} token_t;

/**
 * @brief Lexer instance
 */
typedef struct {
    const char *input;
    size_t input_length;
    size_t position;
    uint32_t line;
    uint32_t column;
    char current_char;
} lexer_t;

/**
 * @brief Create a new lexer
 * @param input SQL input string
 * @param length Input length
 * @return New lexer or NULL on error
 */
lexer_t *lexer_create(const char *input, size_t length);

/**
 * @brief Destroy a lexer
 * @param lexer Lexer instance
 */
void lexer_destroy(lexer_t *lexer);

/**
 * @brief Get next token
 * @param lexer Lexer instance
 * @return Token
 */
token_t lexer_next_token(lexer_t *lexer);

/**
 * @brief Peek next token without consuming
 * @param lexer Lexer instance
 * @return Token (caller must free)
 */
token_t *lexer_peek_token(lexer_t *lexer);

/**
 * @brief Consume a specific token
 * @param lexer Lexer instance
 * @param expected_type Expected token type
 * @return Token or error token if doesn't match
 */
token_t lexer_consume(lexer_t *lexer, token_type_t expected_type);

/**
 * @brief Get current token (lookahead)
 * @param lexer Lexer instance
 * @return Current token type
 */
token_type_t lexer_current_token(lexer_t *lexer);

/**
 * @brief Check if current token matches type
 * @param lexer Lexer instance
 * @param type Token type to check
 * @return true if matches
 */
bool lexer_match(lexer_t *lexer, token_type_t type);

/**
 * @brief Check if current token is in set
 * @param lexer Lexer instance
 * @param types Array of token types
 * @param num_types Number of types
 * @return true if matches any
 */
bool lexer_match_any(lexer_t *lexer, token_type_t *types, size_t num_types);

/**
 * @brief Get token type as string
 * @param type Token type
 * @return String representation
 */
const char *token_type_to_string(token_type_t type);

/**
 * @brief Free a token
 * @param token Token to free
 */
void token_free(token_t *token);

#endif /* SQL_ENGINE_PARSER_LEXER_H */

/** @} */ /* lexer */
