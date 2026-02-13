/**
 * @file cloudSQL_tests.cpp
 * @brief Comprehensive test suite for cloudSQL C++ implementation
 */

#include <iostream>
#include <string>
#include <memory>
#include <vector>
#include <cassert>
#include <cstdio>
#include <thread>
#include <chrono>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>

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

// ============= Value Tests =============

TEST(ValueTest_Basic) {
    auto val = Value::make_int64(42);
    EXPECT_EQ(val.to_int64(), 42);
}

TEST(ValueTest_TypeVariety) {
    Value b(true);
    EXPECT_TRUE(b.as_bool());
    EXPECT_STREQ(b.to_string().c_str(), "TRUE");

    Value f(3.14159);
    EXPECT_TRUE(f.as_float64() > 3.14 && f.as_float64() < 3.15);

    Value s("cloudSQL");
    EXPECT_STREQ(s.as_text().c_str(), "cloudSQL");
}

// ============= Parser Tests =============

TEST(ParserTest_Expressions) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT 1 + 2 * 3 FROM dual");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        EXPECT_TRUE(stmt != nullptr);
        auto select = static_cast<SelectStatement*>(stmt.get());
        EXPECT_STREQ(select->columns()[0]->to_string().c_str(), "1 + 2 * 3");
    }
}

TEST(ParserTest_SelectVariants) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT DISTINCT name FROM users LIMIT 10 OFFSET 20");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto select = static_cast<SelectStatement*>(stmt.get());
        EXPECT_TRUE(select->distinct());
        EXPECT_EQ(select->limit(), 10);
        EXPECT_EQ(select->offset(), 20);
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT age, cnt FROM users GROUP BY age ORDER BY age");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        auto select = static_cast<SelectStatement*>(stmt.get());
        EXPECT_EQ(select->group_by().size(), static_cast<size_t>(1));
        EXPECT_EQ(select->order_by().size(), static_cast<size_t>(1));
    }
}

TEST(ParserTest_Errors) {
    {
        auto lexer = std::make_unique<Lexer>("SELECT FROM users");
        Parser parser(std::move(lexer));
        auto stmt = parser.parse_statement();
        EXPECT_TRUE(stmt == nullptr);
    }
}

// ============= Storage Tests =============

TEST(StorageTest_Persistence) {
    std::string filename = "persist_test";
    std::remove("./test_data/persist_test.heap");
    Schema schema;
    schema.add_column("data", TYPE_TEXT);
    {
        StorageManager sm("./test_data");
        HeapTable table(filename, sm, schema);
        table.create();
        table.insert(Tuple({Value::make_text("Persistent data")}));
    }
    {
        StorageManager sm("./test_data");
        HeapTable table(filename, sm, schema);
        auto iter = table.scan();
        Tuple t;
        EXPECT_TRUE(iter.next(t));
        EXPECT_STREQ(t.get(0).as_text().c_str(), "Persistent data");
    }
}

TEST(StorageTest_Delete) {
    std::string filename = "delete_test";
    std::remove("./test_data/delete_test.heap");
    StorageManager sm("./test_data");
    Schema schema;
    schema.add_column("id", TYPE_INT64);
    HeapTable table(filename, sm, schema);
    EXPECT_TRUE(table.create());

    auto tid1 = table.insert(Tuple({Value::make_int64(1)}));
    table.insert(Tuple({Value::make_int64(2)}));
    
    EXPECT_EQ(table.tuple_count(), 2);
    EXPECT_TRUE(table.remove(tid1));
    EXPECT_EQ(table.tuple_count(), 1);

    auto iter = table.scan();
    Tuple t;
    EXPECT_TRUE(iter.next(t));
    EXPECT_EQ(t.get(0).to_int64(), 2);
    EXPECT_FALSE(iter.next(t));
}

// ============= Index Tests =============

TEST(IndexTest_BTreeBasic) {
    std::remove("./test_data/idx_test.idx");
    StorageManager sm("./test_data");
    BTreeIndex idx("idx_test", sm, TYPE_INT64);
    idx.create();
    idx.insert(Value::make_int64(10), HeapTable::TupleId(1, 1));
    idx.insert(Value::make_int64(20), HeapTable::TupleId(1, 2));
    idx.insert(Value::make_int64(10), HeapTable::TupleId(2, 1));
    auto res = idx.search(Value::make_int64(10));
    EXPECT_EQ(res.size(), static_cast<size_t>(2));
    idx.drop();
}

// ============= Network Tests =============

TEST(NetworkTest_Handshake) {
    uint16_t port = 5438;
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    auto server = network::Server::create(port, *catalog, sm);
    
    std::thread server_thread([&]() {
        server->start();
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        uint32_t ssl_req[] = {htonl(8), htonl(80877103)};
        send(sock, ssl_req, 8, 0);
        char response;
        recv(sock, &response, 1, 0);
        EXPECT_EQ(response, 'N');

        uint32_t startup[] = {htonl(8), htonl(196608)};
        send(sock, startup, 8, 0);
        char type;
        recv(sock, &type, 1, 0);
        EXPECT_EQ(type, 'R');
    }

    close(sock);
    server->stop();
    if (server_thread.joinable()) server_thread.join();
}

// ============= Execution Tests =============

TEST(ExecutionTest_EndToEnd) {
    std::remove("./test_data/users.heap");
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    QueryExecutor exec(*catalog, sm);

    {
        auto lexer = std::make_unique<Lexer>("CREATE TABLE users (id BIGINT, age BIGINT)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        exec.execute(*stmt);
    }
    {
        auto lexer = std::make_unique<Lexer>("INSERT INTO users (id, age) VALUES (1, 20), (2, 30), (3, 40)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        exec.execute(*stmt);
    }
    {
        auto lexer = std::make_unique<Lexer>("SELECT id FROM users WHERE age > 25");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        auto res = exec.execute(*stmt);
        EXPECT_EQ(res.row_count(), 2);
    }
}

TEST(ExecutionTest_Sort) {
    std::remove("./test_data/sort_test.heap");
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    QueryExecutor exec(*catalog, sm);

    exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE sort_test (val INT)")).parse_statement());
    exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO sort_test VALUES (30), (10), (20)")).parse_statement());

    auto res = exec.execute(*Parser(std::make_unique<Lexer>("SELECT val FROM sort_test ORDER BY val")).parse_statement());
    EXPECT_EQ(res.row_count(), 3);
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "10");
    EXPECT_STREQ(res.rows()[1].get(0).to_string().c_str(), "20");
    EXPECT_STREQ(res.rows()[2].get(0).to_string().c_str(), "30");
}

TEST(ExecutionTest_Aggregate) {
    std::remove("./test_data/agg_test.heap");
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    QueryExecutor exec(*catalog, sm);

    exec.execute(*Parser(std::make_unique<Lexer>("CREATE TABLE agg_test (cat TEXT, val INT)")).parse_statement());
    exec.execute(*Parser(std::make_unique<Lexer>("INSERT INTO agg_test VALUES ('A', 10), ('A', 20), ('B', 5)")).parse_statement());

    auto res = exec.execute(*Parser(std::make_unique<Lexer>("SELECT cat, COUNT(val), SUM(val) FROM agg_test GROUP BY cat")).parse_statement());
    EXPECT_EQ(res.row_count(), 2);
    /* Groups: 'A' (count 2, sum 30), 'B' (count 1, sum 5) */
    /* Due to std::map implementation in AggregateOperator, they will be sorted by key 'A|', 'B|' */
    EXPECT_STREQ(res.rows()[0].get(0).to_string().c_str(), "2"); /* count for A */
    EXPECT_STREQ(res.rows()[0].get(1).to_string().c_str(), "30.0"); /* sum for A */
}

int main() {
    std::cout << "cloudSQL C++ Test Suite" << std::endl;
    std::cout << "========================" << std::endl << std::endl;
    
    RUN_TEST(ValueTest_Basic);
    RUN_TEST(ValueTest_TypeVariety);
    RUN_TEST(ParserTest_Expressions);
    RUN_TEST(ParserTest_SelectVariants);
    RUN_TEST(ParserTest_Errors);
    RUN_TEST(StorageTest_Persistence);
    RUN_TEST(StorageTest_Delete);
    RUN_TEST(IndexTest_BTreeBasic);
    RUN_TEST(NetworkTest_Handshake);
    RUN_TEST(ExecutionTest_EndToEnd);
    RUN_TEST(ExecutionTest_Sort);
    RUN_TEST(ExecutionTest_Aggregate);
    
    std::cout << std::endl << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed" << std::endl;
    
    return (tests_failed > 0);
}
