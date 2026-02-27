/**
 * @file buffer_pool_tests.cpp
 * @brief Unit tests for Buffer Pool Manager
 */

#include <cstdio>
#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <cstdint>

#include "storage/buffer_pool_manager.hpp"
#include "storage/storage_manager.hpp"
#include "storage/page.hpp"

using namespace cloudsql::storage;

namespace {

constexpr size_t HELLO_LEN = 6;

TEST(BufferPoolTests, Basic) {
    const std::string filename = "test.db";
    static_cast<void>(std::remove(filename.c_str()));

    StorageManager disk_manager(".");
    BufferPoolManager bpm(3, disk_manager);

    EXPECT_TRUE(bpm.open_file(filename));

    const uint32_t page_id1 = 0;
    Page* const page1 = bpm.new_page(filename, &page_id1);
    ASSERT_NE(page1, nullptr);
    EXPECT_EQ(page_id1, 0);

    std::memcpy(page1->get_data(), "Hello", HELLO_LEN);
    bpm.unpin_page(filename, page_id1, true);

    const uint32_t page_id2 = 1;
    const Page* const page2 = bpm.new_page(filename, &page_id2);
    ASSERT_NE(page2, nullptr);
    EXPECT_EQ(page_id2, 1);
    bpm.unpin_page(filename, page_id2, false);

    const uint32_t page_id3 = 2;
    const Page* const page3 = bpm.new_page(filename, &page_id3);
    ASSERT_NE(page3, nullptr);
    EXPECT_EQ(page_id3, 2);
    bpm.unpin_page(filename, page_id3, false);

    // Fetch page 1 again
    Page* const page1_fetch = bpm.fetch_page(filename, page_id1);
    ASSERT_NE(page1_fetch, nullptr);
    EXPECT_STREQ(page1_fetch->get_data(), "Hello");
    bpm.unpin_page(filename, page_id1, false);

    static_cast<void>(bpm.close_file(filename));
    static_cast<void>(std::remove(filename.c_str()));
}

}  // namespace
