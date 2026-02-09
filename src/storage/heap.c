/**
 * @file heap.c
 * @brief Heap file storage implementation
 *
 * @defgroup heap Heap File Storage
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#include "storage/heap.h"
#include "storage/manager.h"

/* Heap file constants */
#define HEAP_SIGNATURE 0x48454150  /* "HEAP" */

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
 * @brief Initialize heap page
 */
static void init_heap_page(page_t *page) {
    heap_page_header_t *header = (heap_page_header_t *)page;
    
    header->lsn = 0;
    header->lower = sizeof(heap_page_header_t);
    header->upper = PAGE_SIZE;
    header->special = PAGE_SIZE;
    header->num_items = 0;
    header->flags = 0;
}

/**
 * @brief Create a new heap file
 */
heap_file_t *heap_create(storage_manager_t *storage, const char *table_name) {
    heap_file_t *heap;
    char filename[PATH_MAX];
    FILE *file;
    
    if (storage == NULL || table_name == NULL) {
        return NULL;
    }
    
    heap = ALLOC_ZERO(sizeof(heap_file_t));
    if (heap == NULL) {
        return NULL;
    }
    
    /* Copy table name */
    strncpy(heap->table_name, table_name, sizeof(heap->table_name) - 1);
    
    /* Build filename */
    snprintf(filename, sizeof(filename), "%s.heap", table_name);
    
    /* Create file */
    file = storage_open_file(storage, filename);
    if (file == NULL) {
        FREE(heap);
        return NULL;
    }
    
    heap->storage = storage;
    heap->file = file;
    heap->num_pages = 0;
    heap->current_page = 0;
    
    /* Allocate page buffer */
    heap->page_buffer = ALLOC_ZERO(sizeof(page_t));
    if (heap->page_buffer == NULL) {
        fclose(file);
        FREE(heap);
        return NULL;
    }
    
    return heap;
}

/**
 * @brief Open an existing heap file
 */
heap_file_t *heap_open(storage_manager_t *storage, const char *table_name) {
    heap_file_t *heap;
    char filename[PATH_MAX];
    FILE *file;
    uint64_t file_size;
    
    if (storage == NULL || table_name == NULL) {
        return NULL;
    }
    
    /* Build filename */
    snprintf(filename, sizeof(filename), "%s.heap", table_name);
    
    /* Open file */
    file = storage_open_file(storage, filename);
    if (file == NULL) {
        return NULL;
    }
    
    file_size = get_file_size(file);
    
    heap = ALLOC_ZERO(sizeof(heap_file_t));
    if (heap == NULL) {
        storage_close_file(storage, file);
        return NULL;
    }
    
    /* Copy table name */
    strncpy(heap->table_name, table_name, sizeof(heap->table_name) - 1);
    
    heap->storage = storage;
    heap->file = file;
    heap->num_pages = (file_size + PAGE_SIZE - 1) / PAGE_SIZE;
    heap->current_page = 0;
    
    /* Allocate page buffer */
    heap->page_buffer = ALLOC_ZERO(sizeof(page_t));
    if (heap->page_buffer == NULL) {
        storage_close_file(storage, file);
        FREE(heap);
        return NULL;
    }
    
    return heap;
}

/**
 * @brief Close a heap file
 */
void heap_close(heap_file_t *heap) {
    if (heap == NULL) {
        return;
    }
    
    /* Flush page buffer */
    if (heap->page_buffer != NULL) {
        FREE(heap->page_buffer);
        heap->page_buffer = NULL;
    }
}

/**
 * @brief Destroy a heap file
 */
void heap_destroy(heap_file_t *heap) {
    if (heap == NULL) {
        return;
    }
    
    heap_close(heap);
    
    if (heap->file != NULL) {
        storage_close_file(heap->storage, heap->file);
    }
    
    FREE(heap);
}

/**
 * @brief Read a page from heap file
 */
static int heap_read_page(heap_file_t *heap, uint32_t page_num, page_t *page) {
    return storage_read_page(heap->storage, heap->file, page_num, page);
}

/**
 * @brief Write a page to heap file
 */
static int heap_write_page(heap_file_t *heap, uint32_t page_num, page_t *page) {
    return storage_write_page(heap->storage, heap->file, page_num, page);
}

/**
 * @brief Find page with enough free space
 */
static uint32_t heap_find_page_with_space(heap_file_t *heap, uint16_t tuple_size) {
    page_t page;
    heap_page_header_t *header;
    uint16_t line_ptr_size = HEAP_LINE_POINTER_SIZE;
    uint16_t free_space;
    
    for (uint32_t page_num = 0; page_num < heap->num_pages; page_num++) {
        if (heap_read_page(heap, page_num, &page) != 0) {
            continue;
        }
        
        header = (heap_page_header_t *)&page;
        free_space = header->upper - header->lower - 
                     (header->num_items * line_ptr_size) - tuple_size;
        
        if (free_space >= tuple_size) {
            return page_num;
        }
    }
    
    /* No page with enough space, return new page */
    return heap->num_pages;
}

/**
 * @brief Insert a tuple into heap
 */
tuple_id_t heap_insert(heap_file_t *heap, tuple_t *tuple) {
    page_t page;
    heap_page_header_t *header;
    heap_line_pointer_t *line_ptr;
    heap_tuple_header_t *tuple_header;
    uint16_t tuple_size;
    uint16_t line_ptr_size = HEAP_LINE_POINTER_SIZE;
    uint32_t page_num;
    tuple_id_t tuple_id = {0, 0};
    
    if (heap == NULL || tuple == NULL) {
        return tuple_id;
    }
    
    /* Calculate tuple size */
    tuple_size = sizeof(heap_tuple_header_t) + tuple->num_attrs * sizeof(value_type_t);
    for (int i = 0; i < tuple->num_attrs; i++) {
        if (tuple->values[i].type == TYPE_VARCHAR || 
            tuple->values[i].type == TYPE_TEXT ||
            tuple->values[i].type == TYPE_JSON) {
            tuple_size += strlen(tuple->values[i].value.string_val) + 1;
        }
    }
    
    /* Find a page with enough space */
    page_num = heap_find_page_with_space(heap, tuple_size);
    
    /* Read or create page */
    if (page_num < heap->num_pages) {
        heap_read_page(heap, page_num, &page);
    } else {
        init_heap_page(&page);
    }
    
    header = (heap_page_header_t *)&page;
    
    /* Check if there's enough space */
    uint16_t free_space = header->upper - header->lower - 
                          (header->num_items * line_ptr_size);
    
    if (free_space < tuple_size + line_ptr_size) {
        /* No space, need new page */
        if (page_num < heap->num_pages) {
            /* Write current page */
            heap_write_page(heap, page_num, &page);
        }
        
        page_num = heap->num_pages;
        init_heap_page(&page);
        header = (heap_page_header_t *)&page;
    }
    
    /* Add line pointer at the beginning of free space */
    line_ptr = (heap_line_pointer_t *)((char *)&page + header->lower);
    line_ptr->offset = header->upper - tuple_size;
    line_ptr->length = tuple_size;
    
    /* Update header */
    header->lower += line_ptr_size;
    header->num_items++;
    
    /* Write tuple at the end of page */
    tuple_header = (heap_tuple_header_t *)((char *)&page + line_ptr->offset);
    tuple_header->t_xmin = 1;  /* TODO: Get actual transaction ID */
    tuple_header->t_xmax = 0;
    tuple_header->t_cid = 0;
    tuple_header->t_ctid = 0;
    tuple_header->t_len = tuple_size;
    tuple_header->t_hoff = sizeof(heap_tuple_header_t);
    
    /* Copy tuple data */
    /* TODO: Copy actual column values */
    
    /* Write page */
    heap_write_page(heap, page_num, &page);
    
    /* Update page count if we added a new page */
    if (page_num >= heap->num_pages) {
        heap->num_pages = page_num + 1;
    }
    
    /* Return tuple ID */
    tuple_id.page_num = page_num;
    tuple_id.slot_num = header->num_items - 1;
    
    return tuple_id;
}

/**
 * @brief Delete a tuple from heap
 */
int heap_delete(heap_file_t *heap, tuple_id_t tuple_id) {
    page_t page;
    heap_page_header_t *header;
    heap_line_pointer_t *line_ptr;
    
    if (heap == NULL) {
        return -1;
    }
    
    if (tuple_id.page_num >= heap->num_pages) {
        return -1;
    }
    
    /* Read page */
    if (heap_read_page(heap, tuple_id.page_num, &page) != 0) {
        return -1;
    }
    
    header = (heap_page_header_t *)&page;
    
    if (tuple_id.slot_num >= header->num_items) {
        return -1;
    }
    
    /* Get line pointer */
    line_ptr = (heap_line_pointer_t *)((char *)&page + sizeof(heap_page_header_t) + 
                                         tuple_id.slot_num * line_ptr_size);
    
    /* Mark tuple as deleted by setting xmax */
    /* TODO: Implement MVCC */
    
    /* Write page */
    heap_write_page(heap, tuple_id.page_num, &page);
    
    return 0;
}

/**
 * @brief Update a tuple in heap
 */
tuple_id_t heap_update(heap_file_t *heap, tuple_id_t tuple_id, tuple_t *new_tuple) {
    tuple_id_t new_id;
    
    if (heap == NULL) {
        new_id.page_num = 0;
        new_id.slot_num = 0;
        return new_id;
    }
    
    /* Delete old tuple */
    heap_delete(heap, tuple_id);
    
    /* Insert new tuple */
    new_id = heap_insert(heap, new_tuple);
    
    /* Update old tuple's ctid to point to new location (for MVCC) */
    /* TODO: Implement MVCC chaining */
    
    return new_id;
}

/**
 * @brief Get a tuple by ID
 */
tuple_t *heap_get(heap_file_t *heap, tuple_id_t tuple_id) {
    page_t page;
    heap_page_header_t *header;
    heap_line_pointer_t *line_ptr;
    heap_tuple_header_t *tuple_header;
    tuple_t *tuple;
    
    if (heap == NULL) {
        return NULL;
    }
    
    if (tuple_id.page_num >= heap->num_pages) {
        return NULL;
    }
    
    /* Read page */
    if (heap_read_page(heap, tuple_id.page_num, &page) != 0) {
        return NULL;
    }
    
    header = (heap_page_header_t *)&page;
    
    if (tuple_id.slot_num >= header->num_items) {
        return NULL;
    }
    
    /* Get line pointer */
    line_ptr = (heap_line_pointer_t *)((char *)&page + sizeof(heap_page_header_t) + 
                                         tuple_id.slot_num * sizeof(heap_line_pointer_t));
    
    /* Check if tuple is visible (MVCC) */
    /* TODO: Implement visibility check */
    
    /* Get tuple header */
    tuple_header = (heap_tuple_header_t *)((char *)&page + line_ptr->offset);
    
    /* Allocate tuple */
    tuple = ALLOC_ZERO(sizeof(tuple_t));
    if (tuple == NULL) {
        return NULL;
    }
    
    tuple->tuple_id = tuple_id;
    /* TODO: Parse tuple data and populate values */
    
    return tuple;
}

/**
 * @brief Begin heap scan
 */
heap_scan_t *heap_begin_scan(heap_file_t *heap) {
    heap_scan_t *scan;
    
    if (heap == NULL) {
        return NULL;
    }
    
    scan = ALLOC_ZERO(sizeof(heap_scan_t));
    if (scan == NULL) {
        return NULL;
    }
    
    scan->heap = heap;
    scan->page_num = 0;
    scan->slot_num = 0;
    scan->eof = false;
    
    return scan;
}

/**
 * @brief Get next tuple in scan
 */
tuple_t *heap_scan_next(heap_scan_t *scan) {
    page_t page;
    heap_page_header_t *header;
    heap_line_pointer_t *line_ptr;
    
    if (scan == NULL || scan->eof) {
        return NULL;
    }
    
    while (scan->page_num < scan->heap->num_pages) {
        /* Read page */
        if (heap_read_page(scan->heap, scan->page_num, &page) != 0) {
            scan->page_num++;
            continue;
        }
        
        header = (heap_page_header_t *)&page;
        
        /* Find next valid tuple */
        while (scan->slot_num < header->num_items) {
            line_ptr = (heap_line_pointer_t *)((char *)&page + sizeof(heap_page_header_t) + 
                                               scan->slot_num * sizeof(heap_line_pointer_t));
            
            scan->slot_num++;
            
            /* TODO: Check visibility */
            
            /* Return tuple */
            tuple_id_t tuple_id = {scan->page_num, scan->slot_num - 1};
            return heap_get(scan->heap, tuple_id);
        }
        
        /* Move to next page */
        scan->page_num++;
        scan->slot_num = 0;
    }
    
    scan->eof = true;
    return NULL;
}

/**
 * @brief End heap scan
 */
void heap_end_scan(heap_scan_t *scan) {
    FREE(scan);
}

/**
 * @brief Get number of tuples
 */
uint64_t heap_get_tuple_count(heap_file_t *heap) {
    page_t page;
    uint64_t count = 0;
    
    if (heap == NULL) {
        return 0;
    }
    
    for (uint32_t page_num = 0; page_num < heap->num_pages; page_num++) {
        if (heap_read_page(heap, page_num, &page) != 0) {
            continue;
        }
        
        heap_page_header_t *header = (heap_page_header_t *)&page;
        count += header->num_items;
    }
    
    return count;
}

/**
 * @brief Get file size
 */
uint64_t heap_get_file_size(heap_file_t *heap) {
    if (heap == NULL) {
        return 0;
    }
    
    return get_file_size(heap->file);
}

/**
 * @brief Get page free space
 */
int heap_get_page_free_space(heap_file_t *heap, uint32_t page_num) {
    page_t page;
    heap_page_header_t *header;
    
    if (heap == NULL || page_num >= heap->num_pages) {
        return -1;
    }
    
    if (heap_read_page(heap, page_num, &page) != 0) {
        return -1;
    }
    
    header = (heap_page_header_t *)&page;
    
    return header->upper - header->lower - 
           (header->num_items * HEAP_LINE_POINTER_SIZE);
}

/** @} */ /* heap */
