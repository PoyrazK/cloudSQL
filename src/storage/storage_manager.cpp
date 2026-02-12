#include "storage/storage_manager.hpp"
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>

namespace cloudsql {
namespace storage {

StorageManager::StorageManager(std::string data_dir) 
    : data_dir_(std::move(data_dir)) {
    create_dir_if_not_exists();
}

StorageManager::~StorageManager() {
    for (auto& pair : open_files_) {
        if (pair.second->is_open()) {
            pair.second->close();
        }
    }
}

bool StorageManager::open_file(const std::string& filename) {
    if (open_files_.find(filename) != open_files_.end()) {
        return true; // Already open
    }

    std::string filepath = data_dir_ + "/" + filename;
    auto file = std::make_unique<std::fstream>();
    file->open(filepath, std::ios::in | std::ios::out | std::ios::binary | std::ios::app);
    
    if (!file->is_open()) {
        // Try creating if it doesn't exist
        file->open(filepath, std::ios::out | std::ios::binary);
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

bool StorageManager::close_file(const std::string& filename) {
    auto it = open_files_.find(filename);
    if (it == open_files_.end()) {
        return false;
    }

    it->second->close();
    open_files_.erase(it);
    return true;
}

bool StorageManager::read_page(const std::string& filename, uint32_t page_num, char* buffer) {
    if (open_files_.find(filename) == open_files_.end()) {
        if (!open_file(filename)) return false;
    }

    auto& file = open_files_[filename];
    file->seekg(page_num * PAGE_SIZE, std::ios::beg);
    if (file->fail()) return false;

    file->read(buffer, PAGE_SIZE);
    
    if (file->gcount() != PAGE_SIZE) {
        // Handle partial read or EOF
        if (file->eof()) {
            file->clear(); // Clear EOF flag
            std::fill(buffer, buffer + PAGE_SIZE, 0); // Zero fill if new page
            return true;
        }
        return false;
    }

    stats_.pages_read++;
    stats_.bytes_read += PAGE_SIZE;
    return true;
}

bool StorageManager::write_page(const std::string& filename, uint32_t page_num, const char* buffer) {
    if (open_files_.find(filename) == open_files_.end()) {
        if (!open_file(filename)) return false;
    }

    auto& file = open_files_[filename];
    file->seekp(page_num * PAGE_SIZE, std::ios::beg);
    if (file->fail()) return false;

    file->write(buffer, PAGE_SIZE);
    if (file->fail()) return false;
    
    file->flush();

    stats_.pages_written++;
    stats_.bytes_written += PAGE_SIZE;
    return true;
}

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
