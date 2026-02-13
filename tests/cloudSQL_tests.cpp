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
#include "parser/parser.hpp"
#include "parser/expression.hpp"
#include "common/config.hpp"
#include "catalog/catalog.hpp"
#include "network/server.hpp"
#include "storage/heap_table.hpp"
#include "storage/btree_index.hpp"
#include "storage/storage_manager.hpp"
#include "executor/operator.hpp"
#include "executor/query_executor.hpp"

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

#define EXPECT_STREQ(a, b) do { \
    std::string _a = (a); std::string _b = (b); \
    if (_a != _b) { \
        throw std::runtime_error("Expected '" + _b + "' but got '" + _a + "'"); \
    } \
} while(0)

// ============= Value Tests =============

TEST(ValueTest_Basic) {
    auto val = Value::make_int64(42);
    EXPECT_EQ(val.to_int64(), 42);
}

// ============= Parser Tests =============

TEST(ParserTest_Expressions) {
    // 1. Arithmetic Precedence
    {
        auto lexer = std::make_unique<Lexer>("SELECT 1 + 2 * 3");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto select = static_cast<SelectStatement*>(stmt.get());
        EXPECT_STREQ(select->columns()[0]->to_string().c_str(), "1 + 2 * 3");
    }

    // 2. Logic and Comparisons
    {
        auto lexer = std::make_unique<Lexer>("SELECT a > 10 OR b <= 5 AND NOT c");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto select = static_cast<SelectStatement*>(stmt.get());
        EXPECT_STREQ(select->columns()[0]->to_string().c_str(), "a > 10 OR b <= 5 AND NOT c");
    }
}

TEST(ParserTest_SelectVariants) {
    // 1. DISTINCT and LIMIT/OFFSET
    {
        auto lexer = std::make_unique<Lexer>("SELECT DISTINCT name FROM users LIMIT 10 OFFSET 20");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto select = static_cast<SelectStatement*>(stmt.get());
        
        EXPECT_TRUE(select->distinct());
        EXPECT_EQ(select->limit(), 10);
        EXPECT_EQ(select->offset(), 20);
    }

    // 2. ORDER BY and GROUP BY
    {
        auto lexer = std::make_unique<Lexer>("SELECT age, cnt FROM users GROUP BY age ORDER BY age");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto select = static_cast<SelectStatement*>(stmt.get());
        
        EXPECT_EQ(select->group_by().size(), static_cast<size_t>(1));
        EXPECT_EQ(select->order_by().size(), static_cast<size_t>(1));
        EXPECT_STREQ(select->group_by()[0]->to_string().c_str(), "age");
    }
}

TEST(ParserTest_CreateTableComplex) {
    auto sql = "CREATE TABLE products (id INT PRIMARY KEY, price DOUBLE NOT NULL, name VARCHAR(255))";
    auto lexer = std::make_unique<Lexer>(sql);
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    
    EXPECT_TRUE(stmt != nullptr);
    auto ct = static_cast<CreateTableStatement*>(stmt.get());
    EXPECT_STREQ(ct->table_name().c_str(), "products");
    EXPECT_EQ(ct->columns().size(), static_cast<size_t>(3));
    EXPECT_TRUE(ct->columns()[0].is_primary_key_);
}

// ============= Execution Tests =============

TEST(ExecutionTest_EndToEnd) {
    std::remove("./test_data/users.heap");
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    QueryExecutor exec(*catalog, sm);

    // 1. Create Table
    {
        auto lexer = std::make_unique<Lexer>("CREATE TABLE users (id BIGINT, age BIGINT)");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto res = exec.execute(*stmt);
        EXPECT_TRUE(res.success());
    }

    // 2. Insert data via SQL
    {
        auto lexer = std::make_unique<Lexer>("INSERT INTO users (id, age) VALUES (1, 20), (2, 30), (3, 40)");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto res = exec.execute(*stmt);
        EXPECT_TRUE(res.success());
        EXPECT_EQ(res.rows_affected(), 3);
    }

    // 3. Select with Filter
    {
        auto lexer = std::make_unique<Lexer>("SELECT id FROM users WHERE age > 25");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto res = exec.execute(*stmt);

        EXPECT_TRUE(res.success());
        EXPECT_EQ(res.row_count(), static_cast<size_t>(2));
        EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "2");
        EXPECT_STREQ(res.rows()[1].get(0).to_string().c_str(), "3");
    }
}

int main() {
    std::cout << "cloudSQL C++ Test Suite" << std::endl;
    std::cout << "========================" << std::endl << std::endl;
    
    RUN_TEST(ValueTest_Basic);
    RUN_TEST(ParserTest_Expressions);
    RUN_TEST(ParserTest_SelectVariants);
    RUN_TEST(ParserTest_CreateTableComplex);
    RUN_TEST(ExecutionTest_EndToEnd);
    
    std::cout << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed" << std::endl;
    
    if (tests_failed > 0) return 1;
    return 0;
}
