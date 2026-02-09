/**
 * @file btree.h
 * @brief B-tree index implementation
 *
 * @defgroup btree B-tree Index
 * @{
 */

#ifndef SQL_ENGINE_STORAGE_BTREE_H
#define SQL_ENGINE_STORAGE_BTREE_H

#include <stdbool.h>
#include <stdint.h>

#include "common/types.h"
#include "storage/manager.h"

/* B-tree constants */
#define BTREE_PAGE_HEADER_SIZE 24
#define BTREE_MAX_KEYS_PER_PAGE 100
#define BTREE_ORDER 100

/**
 * @brief B-tree page types
 */
typedef enum {
    BTREE_PAGE_ROOT = 0,
    BTREE_PAGE_INTERNAL = 1,
    BTREE_PAGE_LEAF = 2
} btree_page_type_t;

/**
 * @brief B-tree page header
 */
typedef struct {
    uint32_t magic;            /* Magic number */
    uint32_t version;          /* Version */
    uint16_t page_type;        /* Page type */
    uint16_t num_keys;         /* Number of keys */
    uint32_t parent;           /* Parent page number */
    int32_t right_sibling;     /* Right sibling page number */
    uint64_t lsn;             /* Page LSN */
    /* Keys and pointers follow */
} btree_page_header_t;

/**
 * @brief B-tree index entry
 */
typedef struct {
    value_type_t key_type;     /* Key type */
    value_t key;               /* Key value */
    uint32_t page_num;         /* Child page or heap pointer */
    tuple_id_t tuple_id;       /* Heap tuple ID (for leaf pages) */
} btree_entry_t;

/**
 * @brief B-tree index instance
 */
typedef struct btree_index_t {
    /** Storage manager reference */
    struct storage_manager_t *storage;
    
    /** File handle */
    FILE *file;
    
    /** Index name */
    char index_name[NAME_MAX];
    
    /** Table name */
    char table_name[NAME_MAX];
    
    /** Indexed column type */
    value_type_t key_type;
    
    /** Root page number */
    uint32_t root_page;
    
    /** Number of pages */
    uint32_t num_pages;
    
    /** Page buffer */
    page_t *page_buffer;
} btree_index_t;

/**
 * @brief B-tree scan instance
 */
typedef struct {
    btree_index_t *index;
    uint32_t page_num;
    int entry_num;
    bool eof;
    value_t *min_key;
    value_t *max_key;
} btree_scan_t;

/**
 * @brief Comparison result
 */
typedef enum {
    BTREE_LESS = -1,
    BTREE_EQUAL = 0,
    BTREE_GREATER = 1
} btree_cmp_t;

/**
 * @brief Create a new B-tree index
 * @param storage Storage manager
 * @param index_name Index name
 * @param table_name Table name
 * @param key_type Indexed column type
 * @return New index or NULL on error
 */
btree_index_t *btree_create(struct storage_manager_t *storage, const char *index_name,
                            const char *table_name, value_type_t key_type);

/**
 * @brief Open an existing B-tree index
 * @param storage Storage manager
 * @param index_name Index name
 * @return New index or NULL on error
 */
btree_index_t *btree_open(struct storage_manager_t *storage, const char *index_name);

/**
 * @brief Close a B-tree index
 * @param index B-tree index
 */
void btree_close(btree_index_t *index);

/**
 * @brief Destroy a B-tree index
 * @param index B-tree index
 */
void btree_destroy(btree_index_t *index);

/**
 * @brief Insert a key into the index
 * @param index B-tree index
 * @param key Key value
 * @param tuple_id Heap tuple ID
 * @return 0 on success, -1 on error
 */
int btree_insert(btree_index_t *index, value_t *key, tuple_id_t tuple_id);

/**
 * @brief Delete a key from the index
 * @param index B-tree index
 * @param key Key value
 * @param tuple_id Heap tuple ID
 * @return 0 on success, -1 on error
 */
int btree_delete(btree_index_t *index, value_t *key, tuple_id_t tuple_id);

/**
 * @brief Search for a key in the index
 * @param index B-tree index
 * @param key Key value
 * @return Array of matching tuple IDs (NULL terminated)
 */
tuple_id_t **btree_search(btree_index_t *index, value_t *key);

/**
 * @brief Search for key range
 * @param index B-tree index
 * @param min_key Minimum key (NULL for -infinity)
 * @param max_key Maximum key (NULL for +infinity)
 * @return Array of matching tuple IDs (NULL terminated)
 */
tuple_id_t **btree_range_search(btree_index_t *index, value_t *min_key, value_t *max_key);

/**
 * @brief Begin index scan
 * @param index B-tree index
 * @return New scan or NULL on error
 */
btree_scan_t *btree_begin_scan(btree_index_t *index);

/**
 * @brief Begin index scan with bounds
 * @param index B-tree index
 * @param min_key Minimum key (NULL for -infinity)
 * @param max_key Maximum key (NULL for +infinity)
 * @return New scan or NULL on error
 */
btree_scan_t *btree_begin_scan_range(btree_index_t *index, value_t *min_key, value_t *max_key);

/**
 * @brief Get next entry in scan
 * @param scan B-tree scan
 * @return Tuple ID or NULL if done
 */
tuple_id_t *btree_scan_next(btree_scan_t *scan);

/**
 * @brief End index scan
 * @param scan B-tree scan
 */
void btree_end_scan(btree_scan_t *scan);

/**
 * @brief Get index statistics
 * @param index B-tree index
 * @param num_entries Output: number of entries
 * @param depth Output: tree depth
 * @param num_pages Output: number of pages
 */
void btree_get_stats(btree_index_t *index, uint64_t *num_entries, int *depth, uint32_t *num_pages);

/**
 * @brief Verify index integrity
 * @param index B-tree index
 * @return true if valid, false otherwise
 */
bool btree_verify(btree_index_t *index);

#endif /* SQL_ENGINE_STORAGE_BTREE_H */

/** @} */ /* btree */
