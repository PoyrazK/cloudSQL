/**
 * @file heap_table.hpp
 * @brief C++ wrapper for heap file storage
 */

#ifndef CLOUDSQL_STORAGE_HEAP_TABLE_HPP
#define CLOUDSQL_STORAGE_HEAP_TABLE_HPP

#include <string>
#include <memory>
#include <vector>
#include <cstdint>
#include "executor/types.hpp"
#include "storage/storage_manager.hpp"

namespace cloudsql {
namespace storage {

/**
 * @brief Heap table storage (row-oriented)
 */
class HeapTable {
public:
    /**
     * @brief Unique identifier for a tuple in the heap
     */
    struct TupleId {
        uint32_t page_num;
        uint16_t slot_num;
        
        TupleId() : page_num(0), slot_num(0) {}
        TupleId(uint32_t page, uint16_t slot) : page_num(page), slot_num(slot) {}
        
        bool is_null() const { return page_num == 0 && slot_num == 0; }
        
        std::string to_string() const {
            return "(" + std::to_string(page_num) + ", " + std::to_string(slot_num) + ")";
        }

        bool operator==(const TupleId& other) const {
            return page_num == other.page_num && slot_num == other.slot_num;
        }
    };

    /**
     * @brief Page header structure
     */
    struct PageHeader {
        uint32_t next_page;
        uint16_t num_slots;
        uint16_t free_space_offset;
        uint16_t flags;
    };
    
    /**
     * @brief Iterator for sequential scanning
     */
    class Iterator {
    private:
        HeapTable& table_;
        TupleId current_id_;
        bool eof_ = false;
        
    public:
        explicit Iterator(HeapTable& table);
        
        bool next(executor::Tuple& out_tuple);
        bool is_done() const { return eof_; }
        const TupleId& current_id() const { return current_id_; }
    };
    
private:
    std::string table_name_;
    std::string filename_;
    StorageManager& storage_manager_;
    executor::Schema schema_;
    
public:
    HeapTable(std::string table_name, StorageManager& storage_manager, executor::Schema schema);
    
    ~HeapTable() = default;
    
    /* Non-copyable */
    HeapTable(const HeapTable&) = delete;
    HeapTable& operator=(const HeapTable&) = delete;
    
    /* Movable */
    HeapTable(HeapTable&&) noexcept = default;
    HeapTable& operator=(HeapTable&&) noexcept = default;
    
    const std::string& table_name() const { return table_name_; }
    const executor::Schema& schema() const { return schema_; }
    
    TupleId insert(const executor::Tuple& tuple);
    bool remove(const TupleId& tuple_id);
    bool update(const TupleId& tuple_id, const executor::Tuple& tuple);
    bool get(const TupleId& tuple_id, executor::Tuple& out_tuple) const;
    
    uint64_t tuple_count() const;
    
    Iterator scan() { return Iterator(*this); }
    
    bool exists() const;
    bool create();
    bool drop();

private:
    /* Low-level page operations */
    bool read_page(uint32_t page_num, char* buffer) const;
    bool write_page(uint32_t page_num, const char* buffer);
};

}  // namespace storage
}  // namespace cloudsql

#endif  // CLOUDSQL_STORAGE_HEAP_TABLE_HPP
