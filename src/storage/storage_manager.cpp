/**
 * @file storage_manager.cpp
 * @brief Storage manager implementation
 *
 * @defgroup storage Storage Manager
 * @{
 */

#include "storage/storage_manager.hpp"
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>
#include <algorithm>

namespace cloudsql {
namespace storage {

/**
 * @brief Construct a new Storage Manager
 */
StorageManager::StorageManager(std::string data_dir) 
    : data_dir_(std::move(data_dir)) {
    create_dir_if_not_exists();
}

/**
 * @brief Destroy the Storage Manager and close all files
 */
StorageManager::~StorageManager() {
    for (auto& pair : open_files_) {
        if (pair.second->is_open()) {
            pair.second->close();
        }
    }
}

/**
 * @brief Open a database file
 */
bool StorageManager::open_file(const std::string& filename) {
    if (open_files_.find(filename) != open_files_.end()) {
        return true; 
    }

    std::string filepath = data_dir_ + "/" + filename;
    auto file = std::make_unique<std::fstream>();
    
    /* Open for read/write in binary mode. Do NOT use std::ios::app as it breaks random access writes. */
    file->open(filepath, std::ios::in | std::ios::out | std::ios::binary);
    
    if (!file->is_open()) {
        /* Fallback: create empty file then reopen */
        file->open(filepath, std::ios::out | std::ios::binary);
        if (!file->is_open()) {
            std::cerr << "Failed to create file: " << filepath << std::endl;
            return false;
        }
        file->close();
        file->open(filepath, std::ios::in | std::ios::out | std::ios::binary);
    }

    if (!file->is_open()) {
        std::cerr << "Failed to open file: " << filepath << std::endl;
        return false;
    }

    open_files_[filename] = std::move(file);
    stats_.files_opened++;
    return true;
}

/**
 * @brief Close a database file
 */
bool StorageManager::close_file(const std::string& filename) {
    auto it = open_files_.find(filename);
    if (it == open_files_.end()) {
        return false;
    }

    it->second->close();
    open_files_.erase(it);
    return true;
}

/**
 * @brief Read a page from storage
 */
bool StorageManager::read_page(const std::string& filename, uint32_t page_num, char* buffer) {
    if (open_files_.find(filename) == open_files_.end()) {
        if (!open_file(filename)) return false;
    }

    auto& file = open_files_[filename];
    file->clear(); // Clear EOF/fail bits before seek
    file->seekg(static_cast<std::streamoff>(page_num) * PAGE_SIZE, std::ios::beg);
    
    if (file->fail()) {
        /* If we sought past the end, it's effectively an empty page for now */
        std::fill(buffer, buffer + PAGE_SIZE, 0);
        return true;
    }

    file->read(buffer, PAGE_SIZE);
    
    if (file->gcount() != PAGE_SIZE) {
        if (file->eof()) {
            /* Partial read at EOF: zero-fill the rest */
            std::fill(buffer + file->gcount(), buffer + PAGE_SIZE, 0);
            file->clear();
            return true;
        }
        return false;
    }

    stats_.pages_read++;
    stats_.bytes_read += PAGE_SIZE;
    return true;
}

/**
 * @brief Write a page to storage
 */
bool StorageManager::write_page(const std::string& filename, uint32_t page_num, const char* buffer) {
    if (open_files_.find(filename) == open_files_.end()) {
        if (!open_file(filename)) return false;
    }

    auto& file = open_files_[filename];
    file->clear(); // Clear bits
    file->seekp(static_cast<std::streamoff>(page_num) * PAGE_SIZE, std::ios::beg);
    
    if (file->fail()) return false;

    file->write(buffer, PAGE_SIZE);
    if (file->fail()) return false;
    
    file->flush();

    stats_.pages_written++;
    stats_.bytes_written += PAGE_SIZE;
    return true;
}

/**
 * @brief Create data directory if it doesn't exist
 */
bool StorageManager::create_dir_if_not_exists() {
    struct stat st;
    if (stat(data_dir_.c_str(), &st) != 0) {
        if (mkdir(data_dir_.c_str(), 0755) != 0) {
            std::cerr << "Failed to create data directory: " << data_dir_ << std::endl;
            return false;
        }
    }
    return true;
}

} // namespace storage
} // namespace cloudsql

/** @} */ /* storage */
