/**
 * @file heap.h
 * @brief Heap file storage implementation
 *
 * @defgroup heap Heap File Storage
 * @{
 */

#ifndef SQL_ENGINE_STORAGE_HEAP_H
#define SQL_ENGINE_STORAGE_HEAP_H

#include <stdbool.h>
#include <stdint.h>

#include "common/types.h"
#include "storage/manager.h"

/* Heap file constants */
#define HEAP_PAGE_HEADER_SIZE 24
#define HEAP_LINE_POINTER_SIZE 8
#define HEAP_MAX_TUPLES_PER_PAGE ((PAGE_SIZE - HEAP_PAGE_HEADER_SIZE) / \
                                   (HEAP_LINE_POINTER_SIZE + HEAP_MAX_TUPLE_SIZE))
#define HEAP_MAX_TUPLE_SIZE (PAGE_SIZE - HEAP_PAGE_HEADER_SIZE - \
                             HEAP_LINE_POINTER_SIZE)

/**
 * @brief Heap page header
 */
typedef struct {
    uint64_t lsn;              /* Last LSN on this page */
    uint16_t lower;            /* Offset to free space start */
    uint16_t upper;            /* Offset to free space end */
    uint16_t special;          /* Special section offset */
    uint16_t num_items;        /* Number of line pointers */
    uint32_t flags;            /* Page flags */
} heap_page_header_t;

/**
 * @brief Heap line pointer (item pointer)
 */
typedef struct {
    uint16_t offset;          /* Tuple offset from page start */
    uint16_t length;          /* Tuple length in bytes */
    uint32_t padding;         /* Padding for alignment */
} heap_line_pointer_t;

/**
 * @brief Heap tuple header
 */
typedef struct {
    uint32_t t_xmin;          /* Insert transaction ID */
    uint32_t t_xmax;          /* Delete transaction ID */
    uint32_t t_cid;           /* Command ID */
    uint32_t t_ctid;          /* Self tuple pointer (for chaining) */
    uint16_t t_len;           /* Tuple length */
    uint16_t t_hoff;          /* Tuple header offset */
    /* Fields follow: null bitmap, variable-length fields */
} heap_tuple_header_t;

/**
 * @brief Heap file instance
 */
typedef struct {
    /** Storage manager reference */
    storage_manager_t *storage;
    
    /** File handle */
    FILE *file;
    
    /** Table name */
    char table_name[NAME_MAX];
    
    /** Number of pages */
    uint32_t num_pages;
    
    /** Page buffer (for writes) */
    page_t *page_buffer;
    
    /** Current page number */
    uint32_t current_page;
} heap_file_t;

/**
 * @brief Heap scan instance
 */
typedef struct {
    heap_file_t *heap;
    uint32_t page_num;
    uint16_t slot_num;
    bool eof;
} heap_scan_t;

/**
 * @brief Create a new heap file
 * @param storage Storage manager
 * @param table_name Table name
 * @return New heap file or NULL on error
 */
heap_file_t *heap_create(storage_manager_t *storage, const char *table_name);

/**
 * @brief Open an existing heap file
 * @param storage Storage manager
 * @param table_name Table name
 * @return New heap file or NULL on error
 */
heap_file_t *heap_open(storage_manager_t *storage, const char *table_name);

/**
 * @brief Close a heap file
 * @param heap Heap file
 */
void heap_close(heap_file_t *heap);

/**
 * @brief Destroy a heap file
 * @param heap Heap file
 */
void heap_destroy(heap_file_t *heap);

/**
 * @brief Insert a tuple into heap
 * @param heap Heap file
 * @param tuple Tuple to insert
 * @return Tuple ID or NULL tuple ID on error
 */
tuple_id_t heap_insert(heap_file_t *heap, tuple_t *tuple);

/**
 * @brief Delete a tuple from heap
 * @param heap Heap file
 * @param tuple_id Tuple ID to delete
 * @return 0 on success, -1 on error
 */
int heap_delete(heap_file_t *heap, tuple_id_t tuple_id);

/**
 * @brief Update a tuple in heap
 * @param heap Heap file
 * @param tuple_id Tuple ID to update
 * @param new_tuple New tuple data
 * @return New tuple ID or NULL tuple ID on error
 */
tuple_id_t heap_update(heap_file_t *heap, tuple_id_t tuple_id, tuple_t *new_tuple);

/**
 * @brief Get a tuple by ID
 * @param heap Heap file
 * @param tuple_id Tuple ID
 * @return Tuple or NULL if not found
 */
tuple_t *heap_get(heap_file_t *heap, tuple_id_t tuple_id);

/**
 * @brief Begin heap scan
 * @param heap Heap file
 * @return New scan instance or NULL on error
 */
heap_scan_t *heap_begin_scan(heap_file_t *heap);

/**
 * @brief Get next tuple in scan
 * @param scan Heap scan
 * @return Tuple or NULL if done
 */
tuple_t *heap_scan_next(heap_scan_t *scan);

/**
 * @brief End heap scan
 * @param scan Heap scan
 */
void heap_end_scan(heap_scan_t *scan);

/**
 * @brief Get number of tuples
 * @param heap Heap file
 * @return Number of tuples
 */
uint64_t heap_get_tuple_count(heap_file_t *heap);

/**
 * @brief Get file size
 * @param heap Heap file
 * @return File size in bytes
 */
uint64_t heap_get_file_size(heap_file_t *heap);

/**
 * @brief Vacuum heap file (reclaim space)
 * @param heap Heap file
 * @return Number of pages reclaimed
 */
uint32_t heap_vacuum(heap_file_t *heap);

/**
 * @brief Get page statistics
 * @param heap Heap file
 * @param page_num Page number
 * @return Free space on page in bytes
 */
int heap_get_page_free_space(heap_file_t *heap, uint32_t page_num);

#endif /* SQL_ENGINE_STORAGE_HEAP_H */

/** @} */ /* heap */
