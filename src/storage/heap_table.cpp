/**
 * @file heap_table.cpp
 * @brief Heap table storage implementation
 */

#include "storage/heap_table.hpp"
#include <cstring>
#include <sstream>

namespace cloudsql {
namespace storage {

HeapTable::HeapTable(std::string table_name, StorageManager& storage_manager, executor::Schema schema)
    : table_name_(std::move(table_name))
    , filename_(table_name_ + ".heap")
    , storage_manager_(storage_manager)
    , schema_(std::move(schema)) 
{}

/**
 * @brief Construct iterator and start at the first page
 */
HeapTable::Iterator::Iterator(HeapTable& table) 
    : table_(table), current_id_(0, 0), eof_(false) {}

/**
 * @brief Get next tuple from scan
 */
bool HeapTable::Iterator::next(executor::Tuple& out_tuple) { 
    if (eof_) return false;

    while (true) {
        if (table_.get(current_id_, out_tuple)) {
            /* Found a tuple, advance slot for next call */
            current_id_.slot_num++;
            return true;
        }

        /* No more tuples on this page, move to next page */
        current_id_.page_num++;
        current_id_.slot_num = 0;

        /* If we fail to read the next page, we are at EOF */
        char buffer[StorageManager::PAGE_SIZE];
        if (!table_.read_page(current_id_.page_num, buffer)) {
            eof_ = true;
            return false;
        }
        
        /* Check if the page is empty/newly created */
        PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
        if (header->num_slots == 0) {
            eof_ = true;
            return false;
        }
    }
}

/**
 * @brief Insert a tuple into the table
 */
HeapTable::TupleId HeapTable::insert(const executor::Tuple& tuple) { 
    uint32_t page_num = 0;
    char buffer[StorageManager::PAGE_SIZE];
    
    while (read_page(page_num, buffer)) {
        PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
        
        /* Initialize header if it's a new/uninitialized page */
        if (header->free_space_offset == 0) {
            header->free_space_offset = sizeof(PageHeader) + (32 * sizeof(uint16_t));
            header->num_slots = 0;
        }

        /* Serialize tuple (simple string format for now) */
        std::string data;
        for (const auto& val : tuple.values()) {
            data += val.to_string() + "|";
        }

        /* Check if it fits */
        uint16_t required = static_cast<uint16_t>(data.size() + 1);
        uint16_t slot_array_size = (header->num_slots + 1) * sizeof(uint16_t);
        
        if (header->free_space_offset + required + slot_array_size <= StorageManager::PAGE_SIZE) {
            /* Calculate position */
            uint16_t offset = header->free_space_offset;
            std::memcpy(buffer + offset, data.c_str(), data.size() + 1);
            
            /* Update slot array */
            uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
            slots[header->num_slots] = offset;
            
            header->num_slots++;
            header->free_space_offset += required;
            
            write_page(page_num, buffer);
            return TupleId(page_num, header->num_slots - 1);
        }
        
        page_num++;
    }

    /* Create new page if needed */
    std::memset(buffer, 0, StorageManager::PAGE_SIZE);
    PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
    header->free_space_offset = sizeof(PageHeader) + (32 * sizeof(uint16_t));
    header->num_slots = 0;
    
    write_page(page_num, buffer);
    return insert(tuple); 
}

bool HeapTable::remove(const TupleId& tuple_id) { 
    (void)tuple_id; 
    return true; 
}

bool HeapTable::update(const TupleId& tuple_id, const executor::Tuple& tuple) { 
    (void)tuple_id; 
    (void)tuple; 
    return true; 
}

/**
 * @brief Get a specific tuple by ID
 */
bool HeapTable::get(const TupleId& tuple_id, executor::Tuple& out_tuple) const { 
    char buffer[StorageManager::PAGE_SIZE];
    if (!read_page(tuple_id.page_num, buffer)) return false;

    PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
    if (header->free_space_offset == 0) return false;
    if (tuple_id.slot_num >= header->num_slots) return false;

    uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
    uint16_t offset = slots[tuple_id.slot_num];
    if (offset == 0) return false;

    /* Deserialize (simple pipe-separated) */
    const char* data = buffer + offset;
    std::string s(data);
    std::stringstream ss(s);
    std::string item;
    std::vector<common::Value> values;
    
    while (std::getline(ss, item, '|')) {
        values.push_back(common::Value::make_text(item));
    }
    
    out_tuple = executor::Tuple(std::move(values));
    return true; 
}

uint64_t HeapTable::tuple_count() const { return 0; }

bool HeapTable::exists() const { 
    return true; 
}

bool HeapTable::create() { 
    return storage_manager_.open_file(filename_); 
}

bool HeapTable::drop() { 
    return storage_manager_.close_file(filename_); 
}

bool HeapTable::read_page(uint32_t page_num, char* buffer) const {
    return storage_manager_.read_page(filename_, page_num, buffer);
}

bool HeapTable::write_page(uint32_t page_num, const char* buffer) {
    return storage_manager_.write_page(filename_, page_num, buffer);
}

}  // namespace storage
}  // namespace cloudsql
