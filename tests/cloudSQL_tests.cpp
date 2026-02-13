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

TEST(ParserTest_Errors) {
    /* Invalid syntax: SELECT without columns or FROM */
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
    EXPECT_TRUE(table.create()); /* Initialize first page */

    auto tid1 = table.insert(Tuple({Value::make_int64(1)}));
    table.insert(Tuple({Value::make_int64(2)}));
    
    EXPECT_EQ(table.tuple_count(), 2);
    
    /* Remove first tuple */
    EXPECT_TRUE(table.remove(tid1));
    EXPECT_EQ(table.tuple_count(), 1);

    /* Verify only second tuple remains in scan */
    auto iter = table.scan();
    Tuple t;
    EXPECT_TRUE(iter.next(t));
    EXPECT_EQ(t.get(0).to_int64(), 2);
    EXPECT_FALSE(iter.next(t));
}

// ============= Network Tests =============

TEST(NetworkTest_Handshake) {
    uint16_t port = 5437;
    StorageManager sm("./test_data");
    auto catalog = Catalog::create();
    auto server = network::Server::create(port, *catalog, sm);
    
    std::thread server_thread([&]() {
        server->start();
    });

    /* Give server a moment to start */
    std::this_thread::sleep_for(std::chrono::milliseconds(200));

    /* Client connection */
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    inet_pton(AF_INET, "127.0.0.1", &addr.sin_addr);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == 0) {
        /* 1. Send SSL Request */
        uint32_t ssl_req[] = {htonl(8), htonl(80877103)};
        send(sock, ssl_req, 8, 0);
        
        char response;
        recv(sock, &response, 1, 0);
        EXPECT_EQ(response, 'N'); /* Expect SSL Deny */

        /* 2. Send StartupMessage (Length 8, Protocol 3.0) */
        uint32_t startup[] = {htonl(8), htonl(196608)};
        send(sock, startup, 8, 0);

        char type;
        recv(sock, &type, 1, 0);
        EXPECT_EQ(type, 'R'); /* Expect Auth OK ('R') */
        
        /* Read length of Auth OK */
        uint32_t auth_len;
        recv(sock, &auth_len, 4, 0);
        
        /* Read Success code (0) */
        uint32_t auth_code;
        recv(sock, &auth_code, 4, 0);
        EXPECT_EQ(ntohl(auth_code), 0);

        /* 3. Read ReadyForQuery ('Z') */
        recv(sock, &type, 1, 0);
        EXPECT_EQ(type, 'Z');
    } else {
        server->stop();
        if (server_thread.joinable()) server_thread.join();
        throw std::runtime_error("Failed to connect to server");
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

    /* 1. Create Table */
    {
        auto lexer = std::make_unique<Lexer>("CREATE TABLE users (id BIGINT, age BIGINT)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        EXPECT_TRUE(stmt != nullptr);
        exec.execute(*stmt);
    }

    /* 2. Insert */
    {
        auto lexer = std::make_unique<Lexer>("INSERT INTO users (id, age) VALUES (1, 20), (2, 30), (3, 40)");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        EXPECT_TRUE(stmt != nullptr);
        exec.execute(*stmt);
    }

    /* 3. Select */
    {
        auto lexer = std::make_unique<Lexer>("SELECT id FROM users WHERE age > 25");
        auto stmt = Parser(std::move(lexer)).parse_statement();
        EXPECT_TRUE(stmt != nullptr);
        auto res = exec.execute(*stmt);
        EXPECT_EQ(res.row_count(), 2);
    }
}

int main() {
    std::cout << "cloudSQL C++ Test Suite" << std::endl;
    std::cout << "========================" << std::endl << std::endl;
    
    RUN_TEST(ValueTest_Basic);
    RUN_TEST(ParserTest_Expressions);
    RUN_TEST(ParserTest_Errors);
    RUN_TEST(StorageTest_Persistence);
    RUN_TEST(StorageTest_Delete);
    RUN_TEST(NetworkTest_Handshake);
    RUN_TEST(ExecutionTest_EndToEnd);
    
    std::cout << std::endl << "========================" << std::endl;
    std::cout << "Results: " << tests_passed << " passed, " << tests_failed << " failed" << std::endl;
    
    return (tests_failed > 0);
}
