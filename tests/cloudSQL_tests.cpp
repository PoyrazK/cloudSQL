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
}

TEST(ValueTest_StringOperations) {
    auto val = Value::make_text("hello");
    EXPECT_EQ(val.type(), TYPE_TEXT);
    EXPECT_STREQ(val.as_text().c_str(), "hello");
}

// ============= Parser Tests =============

TEST(ParserTest_SelectStatement) {
    auto lexer = std::make_unique<Lexer>("SELECT id, name FROM users WHERE id = 1");
    Parser parser(std::move(lexer));
    auto stmt = parser.parse_statement();
    
    EXPECT_TRUE(stmt != nullptr);
    EXPECT_EQ(static_cast<int>(stmt->type()), static_cast<int>(StmtType::Select));
    
    auto select = static_cast<SelectStatement*>(stmt.get());
    EXPECT_EQ(select->columns().size(), static_cast<size_t>(2));
    EXPECT_TRUE(select->from() != nullptr);
    EXPECT_TRUE(select->where() != nullptr);
}

// ============= Execution Tests =============

TEST(ExecutionTest_EndToEnd) {
    std::remove("./test_data/users.heap");
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    QueryExecutor exec(*catalog, sm);

    // 1. Create Table
    auto lexer1 = std::make_unique<Lexer>("CREATE TABLE users (id BIGINT, age BIGINT)");
    Parser parser1(std::move(lexer1));
    auto stmt1 = parser1.parse_statement();
    auto res1 = exec.execute(*stmt1);
    EXPECT_TRUE(res1.success());

    // 2. Insert data (manually for now as INSERT parsing is TODO)
    auto table_meta = catalog->get_table_by_name("users");
    Schema schema;
    schema.add_column("id", TYPE_INT64);
    schema.add_column("age", TYPE_INT64);
    HeapTable table("users", sm, schema);
    table.insert(Tuple({Value::make_int64(1), Value::make_int64(20)}));
    table.insert(Tuple({Value::make_int64(2), Value::make_int64(30)}));

    // 3. Select with Filter
    auto lexer2 = std::make_unique<Lexer>("SELECT id FROM users WHERE age > 25");
    Parser parser2(std::move(lexer2));
    auto stmt2 = parser2.parse_statement();
    auto res2 = exec.execute(*stmt2);

    EXPECT_TRUE(res2.success());
    EXPECT_EQ(res2.row_count(), static_cast<size_t>(1));
    EXPECT_STREQ(res2.rows()[0].get(0).to_string().c_str(), "2");
}

int main() {
    std::cout << "cloudSQL C++ Test Suite" << std::endl;
    std::cout << "========================" << std::endl << std::endl;
    
    std::cout << "Value Tests:" << std::endl;
    RUN_TEST(ValueTest_IntegerOperations);
    RUN_TEST(ValueTest_StringOperations);
    std::cout << std::endl;
    
    std::cout << "Parser Tests:" << std::endl;
    RUN_TEST(ParserTest_SelectStatement);
    std::cout << std::endl;

    std::cout << "Execution Tests:" << std::endl;
    RUN_TEST(ExecutionTest_EndToEnd);
    std::cout << std::endl;
    
    std::cout << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed" << std::endl;
    
    if (tests_failed > 0) return 1;
    return 0;
}
