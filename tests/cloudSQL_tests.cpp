/**
 * @file cloudSQL_tests.cpp
 * @brief Simple test suite for cloudSQL C++ implementation
 */

#include <iostream>
#include <string>
#include <memory>
#include <vector>
#include <cassert>
#include <cstdio>
#include "common/value.hpp"
#include "parser/token.hpp"
#include "parser/lexer.hpp"
#include "parser/expression.hpp"
#include "common/config.hpp"
#include "catalog/catalog.hpp"
#include "network/server.hpp"
#include "storage/heap_table.hpp"
#include "storage/storage_manager.hpp"
#include "executor/operator.hpp"

using namespace cloudsql;
using namespace cloudsql::common;
using namespace cloudsql::parser;
using namespace cloudsql::executor;
using namespace cloudsql::storage;

// Simple test framework
static int tests_passed = 0;
static int tests_failed = 0;

#define TEST(name) void test_##name()
#define RUN_TEST(name) do { \
    std::cout << "  " << #name << "... "; \
    try { \
        test_##name(); \
        std::cout << "PASSED" << std::endl; \
        tests_passed++; \
    } catch (const std::exception& e) { \
        std::cout << "FAILED: " << e.what() << std::endl; \
        tests_failed++; \
    } \
} while(0)

#define EXPECT_EQ(a, b) do { \
    auto _a = (a); auto _b = (b); \
    if (_a != _b) { \
        throw std::runtime_error("Expected " + std::to_string(static_cast<long long>(_b)) + " but got " + std::to_string(static_cast<long long>(_a))); \
    } \
} while(0)

#define EXPECT_TRUE(a) do { \
    if (!(a)) { \
        throw std::runtime_error("Expected true but got false"); \
    } \
} while(0)

#define EXPECT_FALSE(a) do { \
    if (a) { \
        throw std::runtime_error("Expected false but got true"); \
    } \
} while(0)

#define EXPECT_STREQ(a, b) do { \
    std::string _a = (a); std::string _b = (b); \
    if (_a != _b) { \
        throw std::runtime_error("Expected '" + _b + "' but got '" + _a + "'"); \
    } \
} while(0)

#define EXPECT_GE(a, b) do { \
    auto _a = (a); auto _b = (b); \
    if (_a < _b) { \
        throw std::runtime_error("Expected " + std::to_string(_a) + " to be >= " + std::to_string(_b)); \
    } \
} while(0)

// ============= Value Tests =============

TEST(ValueTest_IntegerOperations) {
    auto val = Value::make_int64(42);
    EXPECT_EQ(val.type(), TYPE_INT64);
    EXPECT_EQ(val.to_int64(), 42);
    EXPECT_FALSE(val.is_null());
}

TEST(ValueTest_StringOperations) {
    auto val = Value::make_text("hello");
    EXPECT_EQ(val.type(), TYPE_TEXT);
    EXPECT_STREQ(val.as_text().c_str(), "hello");
    EXPECT_FALSE(val.is_null());
}

TEST(ValueTest_NullValue) {
    auto val = Value::make_null();
    EXPECT_EQ(val.type(), TYPE_NULL);
    EXPECT_TRUE(val.is_null());
}

TEST(ValueTest_FloatOperations) {
    auto val = Value::make_float64(3.14);
    EXPECT_EQ(val.type(), TYPE_FLOAT64);
    EXPECT_TRUE(val.to_float64() > 3.13 && val.to_float64() < 3.15);
}

TEST(ValueTest_CopyOperations) {
    auto val1 = Value::make_int64(100);
    auto val2 = val1;  // Copy constructor
    EXPECT_EQ(val2.to_int64(), 100);
    
    auto val3 = std::move(val1);  // Move constructor
    EXPECT_EQ(val3.to_int64(), 100);
}

// ============= Token Tests =============

TEST(TokenTest_BasicTokens) {
    Token tok1(TokenType::Select);
    EXPECT_EQ(static_cast<int>(tok1.type()), static_cast<int>(TokenType::Select));
    
    Token tok2(TokenType::Number, "123");
    EXPECT_EQ(static_cast<int>(tok2.type()), static_cast<int>(TokenType::Number));
    EXPECT_STREQ(tok2.lexeme().c_str(), "123");
}

TEST(TokenTest_IdentifierToken) {
    Token tok(TokenType::Identifier, "users");
    EXPECT_EQ(static_cast<int>(tok.type()), static_cast<int>(TokenType::Identifier));
    EXPECT_STREQ(tok.lexeme().c_str(), "users");
}

TEST(TokenTest_Equality) {
    Token tok1(TokenType::From, "FROM");
    Token tok2(TokenType::From, "FROM");
    Token tok3(TokenType::Where, "WHERE");
    
    EXPECT_TRUE(tok1.type() == tok2.type());
    EXPECT_FALSE(tok1.type() == tok3.type());
}

// ============= Lexer Tests =============

TEST(LexerTest_SelectKeyword) {
    Lexer lexer("SELECT * FROM users");
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    EXPECT_GE(tokens.size(), static_cast<size_t>(4));
    EXPECT_EQ(static_cast<int>(tokens[0].type()), static_cast<int>(TokenType::Select));
}

TEST(LexerTest_Numbers) {
    Lexer lexer("SELECT 123, 456 FROM users");
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    EXPECT_EQ(static_cast<int>(tokens[1].type()), static_cast<int>(TokenType::Number));
    EXPECT_STREQ(tokens[1].lexeme().c_str(), "123");
}

TEST(LexerTest_Strings) {
    Lexer lexer("SELECT 'hello world' FROM users");
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    EXPECT_EQ(static_cast<int>(tokens[1].type()), static_cast<int>(TokenType::String));
}

TEST(LexerTest_Operators) {
    Lexer lexer("WHERE age = 25 AND status = 'active'");
    std::vector<Token> tokens;
    while (!lexer.is_at_end()) {
        tokens.push_back(lexer.next_token());
    }
    
    EXPECT_EQ(static_cast<int>(tokens[0].type()), static_cast<int>(TokenType::Where));
    EXPECT_EQ(static_cast<int>(tokens[2].type()), static_cast<int>(TokenType::Eq));
}

// ============= Expression Tests =============

TEST(ExpressionTest_ConstantExpression) {
    auto expr = std::make_unique<ConstantExpr>(Value::make_int64(42));
    EXPECT_EQ(static_cast<int>(expr->type()), static_cast<int>(ExprType::Constant));
}

TEST(ExpressionTest_ColumnExpression) {
    auto expr = std::make_unique<ColumnExpr>("users", "name");
    EXPECT_EQ(static_cast<int>(expr->type()), static_cast<int>(ExprType::Column));
    EXPECT_STREQ(expr->table().c_str(), "users");
    EXPECT_STREQ(expr->name().c_str(), "name");
}

// ============= Config Tests =============

TEST(ConfigTest_DefaultValues) {
    cloudsql::config::Config config;
    EXPECT_EQ(config.port, cloudsql::config::Config::DEFAULT_PORT);
}

// ============= Execution Tests =============

TEST(ExecutionTest_HeapTableScan) {
    // Cleanup old test data
    std::remove("./test_data/test_table.heap");

    StorageManager sm("./test_data");
    Schema schema;
    schema.add_column("id", TYPE_INT64);
    schema.add_column("name", TYPE_TEXT);

    HeapTable table("test_table", sm, schema);
    table.create();

    // Insert rows
    table.insert(Tuple({Value::make_int64(1), Value::make_text("Alice")}));
    table.insert(Tuple({Value::make_int64(2), Value::make_text("Bob")}));

    // Re-open to ensure persistence
    auto scan = std::make_unique<SeqScanOperator>(std::make_unique<HeapTable>("test_table", sm, schema));
    scan->open();

    Tuple t;
    int count = 0;
    while (scan->next(t)) {
        count++;
        // std::cout << "DEBUG: Row " << count << ": " << t.to_string() << std::endl;
        if (count == 1) EXPECT_STREQ(t.get(0).to_string().c_str(), "1");
        if (count == 2) EXPECT_STREQ(t.get(0).to_string().c_str(), "2");
    }
    EXPECT_EQ(count, 2);
    
    table.drop();
}

int main() {
    std::cout << "cloudSQL C++ Test Suite" << std::endl;
    std::cout << "========================" << std::endl << std::endl;
    
    std::cout << "Value Tests:" << std::endl;
    RUN_TEST(ValueTest_IntegerOperations);
    RUN_TEST(ValueTest_StringOperations);
    RUN_TEST(ValueTest_NullValue);
    RUN_TEST(ValueTest_FloatOperations);
    RUN_TEST(ValueTest_CopyOperations);
    std::cout << std::endl;
    
    std::cout << "Token Tests:" << std::endl;
    RUN_TEST(TokenTest_BasicTokens);
    RUN_TEST(TokenTest_IdentifierToken);
    RUN_TEST(TokenTest_Equality);
    std::cout << std::endl;
    
    std::cout << "Lexer Tests:" << std::endl;
    RUN_TEST(LexerTest_SelectKeyword);
    RUN_TEST(LexerTest_Numbers);
    RUN_TEST(LexerTest_Strings);
    RUN_TEST(LexerTest_Operators);
    std::cout << std::endl;
    
    std::cout << "Expression Tests:" << std::endl;
    RUN_TEST(ExpressionTest_ConstantExpression);
    RUN_TEST(ExpressionTest_ColumnExpression);
    std::cout << std::endl;
    
    std::cout << "Config Tests:" << std::endl;
    RUN_TEST(ConfigTest_DefaultValues);
    std::cout << std::endl;

    std::cout << "Execution Tests:" << std::endl;
    RUN_TEST(ExecutionTest_HeapTableScan);
    std::cout << std::endl;
    
    std::cout << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed" << std::endl;
    
    if (tests_failed > 0) {
        return 1;
    }
    
    return 0;
}
