/**
 * @file btree.c
 * @brief B-tree index implementation
 *
 * @defgroup btree B-tree Index
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "storage/btree.h"
#include "storage/manager.h"

/* B-tree constants */
#define BTREE_MAGIC 0x42545245  /* "BTRE" */
#define BTREE_VERSION 1

/**
 * @brief Compare two keys
 */
static btree_cmp_t btree_compare(value_t *a, value_t *b) {
    if (a == NULL || b == NULL) {
        return BTREE_LESS;
    }
    
    switch (a->type) {
        case TYPE_INT32:
            if (a->value.int32_val < b->value.int32_val) return BTREE_LESS;
            if (a->value.int32_val > b->value.int32_val) return BTREE_GREATER;
            return BTREE_EQUAL;
        case TYPE_INT64:
            if (a->value.int64_val < b->value.int64_val) return BTREE_LESS;
            if (a->value.int64_val > b->value.int64_val) return BTREE_GREATER;
            return BTREE_EQUAL;
        case TYPE_FLOAT64:
            if (a->value.float64_val < b->value.float64_val) return BTREE_LESS;
            if (a->value.float64_val > b->value.float64_val) return BTREE_GREATER;
            return BTREE_EQUAL;
        case TYPE_TEXT:
        case TYPE_VARCHAR:
            if (strcmp(a->value.string_val, b->value.string_val) < 0) return BTREE_LESS;
            if (strcmp(a->value.string_val, b->value.string_val) > 0) return BTREE_GREATER;
            return BTREE_EQUAL;
        default:
            return BTREE_EQUAL;
    }
}

/**
 * @brief Get file size
 */
static uint64_t get_file_size(FILE *file) {
    struct stat st;
    
    if (fstat(fileno(file), &st) < 0) {
        return 0;
    }
    
    return st.st_size;
}

/**
 * @brief Initialize B-tree page
 */
static void init_btree_page(page_t *page, uint16_t page_type) {
    btree_page_header_t *header = (btree_page_header_t *)page;
    
    header->magic = BTREE_MAGIC;
    header->version = BTREE_VERSION;
    header->page_type = page_type;
    header->num_keys = 0;
    header->parent = 0;
    header->right_sibling = -1;
    header->lsn = 0;
}

/**
 * @brief Create a new B-tree index
 */
btree_index_t *btree_create(struct storage_manager_t *storage, const char *index_name,
                            const char *table_name, value_type_t key_type) {
    btree_index_t *index;
    char filename[PATH_MAX];
    FILE *file;
    page_t page;
    
    if (storage == NULL || index_name == NULL || table_name == NULL) {
        return NULL;
    }
    
    index = ALLOC_ZERO(sizeof(btree_index_t));
    if (index == NULL) {
        return NULL;
    }
    
    /* Copy names */
    strncpy(index->index_name, index_name, sizeof(index->index_name) - 1);
    strncpy(index->table_name, table_name, sizeof(index->table_name) - 1);
    index->key_type = key_type;
    
    /* Build filename */
    snprintf(filename, sizeof(filename), "%s_%s.idx", table_name, index_name);
    
    /* Create file */
    file = storage_open_file(storage, filename);
    if (file == NULL) {
        FREE(index);
        return NULL;
    }
    
    /* Initialize root page */
    init_btree_page(&page, BTREE_PAGE_ROOT);
    fwrite(&page, sizeof(page), 1, file);
    
    index->storage = storage;
    index->file = file;
    index->root_page = 0;
    index->num_pages = 1;
    index->page_buffer = ALLOC_ZERO(sizeof(page_t));
    
    return index;
}

/**
 * @brief Open an existing B-tree index
 */
btree_index_t *btree_open(struct storage_manager_t *storage, const char *index_name) {
    btree_index_t *index;
    char filename[PATH_MAX];
    FILE *file;
    uint64_t file_size;
    page_t page;
    btree_page_header_t *header;
    
    if (storage == NULL || index_name == NULL) {
        return NULL;
    }
    
    /* Try to find index file */
    /* TODO: Maintain index catalog */
    return NULL;
}

/**
 * @brief Close a B-tree index
 */
void btree_close(btree_index_t *index) {
    if (index == NULL) {
        return;
    }
    
    if (index->page_buffer != NULL) {
        FREE(index->page_buffer);
        index->page_buffer = NULL;
    }
}

/**
 * @brief Destroy a B-tree index
 */
void btree_destroy(btree_index_t *index) {
    if (index == NULL) {
        return;
    }
    
    btree_close(index);
    
    if (index->file != NULL) {
        storage_close_file(index->storage, index->file);
    }
    
    FREE(index);
}

/**
 * @brief Split a page
 */
static uint32_t btree_split_page(btree_index_t *index, page_t *page, int split_point) {
    page_t new_page;
    btree_page_header_t *old_header = (btree_page_header_t *)page;
    btree_page_header_t *new_header = (btree_page_header_t *)&new_page;
    
    /* TODO: Implement page split */
    
    return index->num_pages;
}

/**
 * @brief Insert a key into the index
 */
int btree_insert(btree_index_t *index, value_t *key, tuple_id_t tuple_id) {
    page_t page;
    btree_page_header_t *header;
    btree_entry_t *entries;
    
    if (index == NULL || key == NULL) {
        return -1;
    }
    
    /* Read root page */
    if (fseeko(index->file, 0, SEEK_SET) != 0) {
        return -1;
    }
    
    if (fread(&page, sizeof(page), 1, index->file) != 1) {
        return -1;
    }
    
    header = (btree_page_header_t *)&page;
    entries = (btree_entry_t *)((char *)&page + BTREE_PAGE_HEADER_SIZE);
    
    /* Check if root page has space */
    if (header->num_keys < BTREE_MAX_KEYS_PER_PAGE) {
        /* Insert in root */
        int pos = header->num_keys;
        
        /* Shift entries */
        memmove(&entries[pos + 1], &entries[pos], 
                header->num_keys * sizeof(btree_entry_t));
        
        /* Insert new entry */
        entries[pos].key_type = index->key_type;
        entries[pos].key = *key;
        entries[pos].tuple_id = tuple_id;
        entries[pos].page_num = 0;
        
        header->num_keys++;
        
        /* Write page */
        fseeko(index->file, 0, SEEK_SET);
        fwrite(&page, sizeof(page), 1, index->file);
        
        return 0;
    }
    
    /* Need to split root */
    /* TODO: Implement full B-tree insert with splits */
    
    return 0;
}

/**
 * @brief Delete a key from the index
 */
int btree_delete(btree_index_t *index, value_t *key, tuple_id_t tuple_id) {
    /* TODO: Implement B-tree delete */
    (void)index;
    (void)key;
    (void)tuple_id;
    return 0;
}

/**
 * @brief Search for a key in the index
 */
tuple_id_t **btree_search(btree_index_t *index, value_t *key) {
    page_t page;
    btree_page_header_t *header;
    btree_entry_t *entries;
    int lo, hi, mid;
    btree_cmp_t cmp;
    
    if (index == NULL || key == NULL) {
        return NULL;
    }
    
    /* Read root page */
    if (fseeko(index->file, 0, SEEK_SET) != 0) {
        return NULL;
    }
    
    if (fread(&page, sizeof(page), 1, index->file) != 1) {
        return NULL;
    }
    
    header = (btree_page_header_t *)&page;
    entries = (btree_entry_t *)((char *)&page + BTREE_PAGE_HEADER_SIZE);
    
    /* Binary search */
    lo = 0;
    hi = header->num_keys - 1;
    
    while (lo <= hi) {
        mid = (lo + hi) / 2;
        cmp = btree_compare(key, &entries[mid].key);
        
        if (cmp == BTREE_EQUAL) {
            /* Found - return tuple ID */
            tuple_id_t **result = ALLOC(sizeof(tuple_id_t *) * 2);
            result[0] = ALLOC(sizeof(tuple_id_t));
            *result[0] = entries[mid].tuple_id;
            result[1] = NULL;
            return result;
        }
        
        if (cmp == BTREE_LESS) {
            hi = mid - 1;
        } else {
            lo = mid + 1;
        }
    }
    
    return NULL;
}

/**
 * @brief Search for key range
 */
tuple_id_t **btree_range_search(btree_index_t *index, value_t *min_key, value_t *max_key) {
    /* TODO: Implement range search */
    (void)index;
    (void)min_key;
    (void)max_key;
    
    /* Placeholder */
    tuple_id_t **result = ALLOC(sizeof(tuple_id_t *) * 2);
    result[0] = NULL;
    result[1] = NULL;
    return result;
}

/**
 * @brief Begin index scan
 */
btree_scan_t *btree_begin_scan(btree_index_t *index) {
    btree_scan_t *scan;
    
    if (index == NULL) {
        return NULL;
    }
    
    scan = ALLOC_ZERO(sizeof(btree_scan_t));
    if (scan == NULL) {
        return NULL;
    }
    
    scan->index = index;
    scan->page_num = 0;
    scan->entry_num = 0;
    scan->eof = false;
    scan->min_key = NULL;
    scan->max_key = NULL;
    
    return scan;
}

/**
 * @brief Begin index scan with bounds
 */
btree_scan_t *btree_begin_scan_range(btree_index_t *index, value_t *min_key, value_t *max_key) {
    btree_scan_t *scan = btree_begin_scan(index);
    
    if (scan == NULL) {
        return NULL;
    }
    
    if (min_key != NULL) {
        scan->min_key = ALLOC(sizeof(value_t));
        *scan->min_key = *min_key;
    }
    
    if (max_key != NULL) {
        scan->max_key = ALLOC(sizeof(value_t));
        *scan->max_key = *max_key;
    }
    
    return scan;
}

/**
 * @brief Get next entry in scan
 */
tuple_id_t *btree_scan_next(btree_scan_t *scan) {
    page_t page;
    btree_page_header_t *header;
    btree_entry_t *entries;
    
    if (scan == NULL || scan->eof) {
        return NULL;
    }
    
    /* Read current page */
    if (fseeko(scan->index->file, scan->page_num * PAGE_SIZE, SEEK_SET) != 0) {
        return NULL;
    }
    
    if (fread(&page, sizeof(page), 1, scan->index->file) != 1) {
        return NULL;
    }
    
    header = (btree_page_header_t *)&page;
    entries = (btree_entry_t *)((char *)&page + BTREE_PAGE_HEADER_SIZE);
    
    while (scan->page_num < scan->index->num_pages) {
        while (scan->entry_num < header->num_keys) {
            btree_entry_t *entry = &entries[scan->entry_num];
            scan->entry_num++;
            
            /* Check bounds */
            if (scan->min_key != NULL) {
                if (btree_compare(&entry->key, scan->min_key) == BTREE_LESS) {
                    continue;
                }
            }
            
            if (scan->max_key != NULL) {
                if (btree_compare(&entry->key, scan->max_key) == BTREE_GREATER) {
                    scan->eof = true;
                    return NULL;
                }
            }
            
            /* Return tuple ID */
            tuple_id_t *result = ALLOC(sizeof(tuple_id_t));
            *result = entry->tuple_id;
            return result;
        }
        
        /* Move to next page */
        scan->page_num++;
        scan->entry_num = 0;
        
        if (scan->page_num < scan->index->num_pages) {
            if (fseeko(scan->index->file, scan->page_num * PAGE_SIZE, SEEK_SET) != 0) {
                return NULL;
            }
            if (fread(&page, sizeof(page), 1, scan->index->file) != 1) {
                return NULL;
            }
            header = (btree_page_header_t *)&page;
            entries = (btree_entry_t *)((char *)&page + BTREE_PAGE_HEADER_SIZE);
        }
    }
    
    scan->eof = true;
    return NULL;
}

/**
 * @brief End index scan
 */
void btree_end_scan(btree_scan_t *scan) {
    if (scan == NULL) {
        return;
    }
    
    if (scan->min_key != NULL) {
        FREE(scan->min_key);
    }
    
    if (scan->max_key != NULL) {
        FREE(scan->max_key);
    }
    
    FREE(scan);
}

/**
 * @brief Get index statistics
 */
void btree_get_stats(btree_index_t *index, uint64_t *num_entries, int *depth, uint32_t *num_pages) {
    if (index == NULL) {
        return;
    }
    
    if (num_entries != NULL) {
        *num_entries = 0;  /* TODO: Count entries */
    }
    
    if (depth != NULL) {
        *depth = 1;  /* TODO: Calculate tree depth */
    }
    
    if (num_pages != NULL) {
        *num_pages = index->num_pages;
    }
}

/**
 * @brief Verify index integrity
 */
bool btree_verify(btree_index_t *index) {
    /* TODO: Implement integrity verification */
    (void)index;
    return true;
}

/** @} */ /* btree */
