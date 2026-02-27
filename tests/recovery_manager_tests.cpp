/**
 * @file recovery_manager_tests.cpp
 * @brief Unit tests for Recovery Manager
 */

#include <gtest/gtest.h>

#include <cstdio>
#include <string>

#include "catalog/catalog.hpp"
#include "recovery/log_manager.hpp"
#include "recovery/recovery_manager.hpp"
#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"

using namespace cloudsql;
using namespace cloudsql::recovery;

namespace {

constexpr size_t TEST_BPM_SIZE = 10;

TEST(RecoveryManagerTests, Basic) {
    const std::string log_file = "recovery_test.log";
    static_cast<void>(std::remove(log_file.c_str()));

    auto catalog = Catalog::create();
    storage::StorageManager disk_manager("./test_data");
    storage::BufferPoolManager bpm(TEST_BPM_SIZE, disk_manager);
    LogManager lm(log_file);

    RecoveryManager rm(bpm, *catalog, lm);
    EXPECT_TRUE(rm.recover());

    static_cast<void>(std::remove(log_file.c_str()));
}

}  // namespace
