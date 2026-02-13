/**
 * @file heap_table.cpp
 * @brief Heap table storage implementation
 */

#include "storage/heap_table.hpp"
#include <cstring>
#include <sstream>
#include <iostream>

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

        /* Check if we reached the end of this page's defined slots */
        char buf[StorageManager::PAGE_SIZE];
        if (table_.read_page(current_id_.page_num, buf)) {
            PageHeader* header = reinterpret_cast<PageHeader*>(buf);
            if (current_id_.slot_num < header->num_slots) {
                /* There are more slots but they are empty/deleted, just increment and continue */
                current_id_.slot_num++;
                continue;
            }
        }

        /* Move to next page */
        current_id_.page_num++;
        current_id_.slot_num = 0;

        /* If we fail to read the next page, we are at EOF */
        if (!table_.read_page(current_id_.page_num, buf)) {
            eof_ = true;
            return false;
        }
        
        /* Check if the page is initialized */
        PageHeader* next_header = reinterpret_cast<PageHeader*>(buf);
        if (next_header->free_space_offset == 0) {
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
    
    while (true) {
        if (!read_page(page_num, buffer)) {
            /* Create new page */
            std::memset(buffer, 0, StorageManager::PAGE_SIZE);
            PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
            header->free_space_offset = sizeof(PageHeader) + (64 * sizeof(uint16_t));
            header->num_slots = 0;
            write_page(page_num, buffer);
        }

        PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
        if (header->free_space_offset == 0) {
            header->free_space_offset = sizeof(PageHeader) + (64 * sizeof(uint16_t));
            header->num_slots = 0;
        }

        std::string data;
        for (const auto& val : tuple.values()) {
            data += val.to_string() + "|";
        }

        uint16_t required = static_cast<uint16_t>(data.size() + 1);
        uint16_t slot_array_end = sizeof(PageHeader) + ((header->num_slots + 1) * sizeof(uint16_t));
        
        if (header->free_space_offset + required < StorageManager::PAGE_SIZE && slot_array_end < header->free_space_offset) {
            /* Fits in this page */
            uint16_t offset = header->free_space_offset;
            std::memcpy(buffer + offset, data.c_str(), data.size() + 1);
            
            uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
            slots[header->num_slots] = offset;
            
            TupleId tid(page_num, header->num_slots);
            header->num_slots++;
            header->free_space_offset += required;
            
            write_page(page_num, buffer);
            return tid;
        }
        
        page_num++;
    }
}

/**
 * @brief Mark a tuple as deleted
 */
bool HeapTable::remove(const TupleId& tuple_id) { 
    char buffer[StorageManager::PAGE_SIZE];
    if (!read_page(tuple_id.page_num, buffer)) return false;

    PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
    if (header->free_space_offset == 0) return false;
    if (tuple_id.slot_num >= header->num_slots) return false;

    uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
    slots[tuple_id.slot_num] = 0; /* Tombstone */

    return write_page(tuple_id.page_num, buffer);
}

bool HeapTable::update(const TupleId& tuple_id, const executor::Tuple& tuple) { 
    if (!remove(tuple_id)) return false;
    insert(tuple);
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

    const char* data = buffer + offset;
    std::string s(data);
    std::stringstream ss(s);
    std::string item;
    std::vector<common::Value> values;
    
    for (size_t i = 0; i < schema_.column_count(); ++i) {
        if (!std::getline(ss, item, '|')) break;
        
        const auto& col = schema_.get_column(i);
        switch (col.type()) {
            case common::TYPE_INT64:
                values.push_back(common::Value::make_int64(std::stoll(item)));
                break;
            case common::TYPE_FLOAT64:
                values.push_back(common::Value::make_float64(std::stod(item)));
                break;
            case common::TYPE_BOOL:
                values.push_back(common::Value::make_bool(item == "TRUE" || item == "1"));
                break;
            default:
                values.push_back(common::Value::make_text(item));
                break;
        }
    }
    
    out_tuple = executor::Tuple(std::move(values));
    return true; 
}

uint64_t HeapTable::tuple_count() const { 
    uint64_t count = 0;
    uint32_t page_num = 0;
    char buffer[StorageManager::PAGE_SIZE];
    while (read_page(page_num, buffer)) {
        PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
        if (header->free_space_offset == 0) break;
        
        uint16_t* slots = reinterpret_cast<uint16_t*>(buffer + sizeof(PageHeader));
        for (int i = 0; i < header->num_slots; ++i) {
            if (slots[i] != 0) count++;
        }
        page_num++;
    }
    return count;
}

bool HeapTable::exists() const { return true; }

bool HeapTable::create() { 
    if (!storage_manager_.open_file(filename_)) return false;
    
    char buffer[StorageManager::PAGE_SIZE];
    std::memset(buffer, 0, StorageManager::PAGE_SIZE);
    PageHeader* header = reinterpret_cast<PageHeader*>(buffer);
    header->free_space_offset = sizeof(PageHeader) + (64 * sizeof(uint16_t));
    header->num_slots = 0;
    
    return write_page(0, buffer);
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
