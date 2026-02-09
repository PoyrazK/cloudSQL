/**
 * @file catalog.c
 * @brief System catalog implementation
 *
 * @defgroup catalog System Catalog
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "catalog/catalog.h"
#include "storage/manager.h"

/**
 * @brief Catalog file magic number
 */
#define CATALOG_MAGIC 0x4341544C  /* "CATL" */
#define CATALOG_VERSION 1

/**
 * @brief Catalog file header
 */
typedef struct {
    uint32_t magic;
    uint32_t version;
    uint64_t last_update;
    oid_t next_table_id;
    oid_t next_index_id;
    uint16_t num_tables;
    uint16_t num_indexes;
} catalog_header_t;

/**
 * @brief Create a new catalog
 */
catalog_t *catalog_create(storage_manager_t *storage) {
    catalog_t *catalog;
    
    if (storage == NULL) {
        return NULL;
    }
    
    catalog = ALLOC_ZERO(sizeof(catalog_t));
    if (catalog == NULL) {
        return NULL;
    }
    
    catalog->storage = storage;
    catalog->catalog_file = NULL;
    
    /* Initialize database info */
    memset(&catalog->database, 0, sizeof(catalog->database));
    strncpy(catalog->database.name, "default", sizeof(catalog->database.name) - 1);
    catalog->database.encoding = 6;  /* UTF8 */
    strncpy(catalog->database.collation, "C", sizeof(catalog->database.collation) - 1);
    catalog->database.num_tables = 0;
    catalog->database.table_ids = NULL;
    catalog->database.created_at = time(NULL);
    
    /* Initialize tables and indexes */
    catalog->num_tables = 0;
    catalog->tables = NULL;
    catalog->indexes = NULL;
    
    /* Initialize metadata */
    catalog->version = 1;
    catalog->last_update = time(NULL);
    
    return catalog;
}

/**
 * @brief Destroy a catalog
 */
void catalog_destroy(catalog_t *catalog) {
    if (catalog == NULL) {
        return;
    }
    
    /* Close catalog file */
    if (catalog->catalog_file != NULL) {
        storage_close_file(catalog->storage, catalog->catalog_file);
    }
    
    /* Free tables */
    for (uint16_t i = 0; i < catalog->num_tables; i++) {
        if (catalog->tables[i] != NULL) {
            /* Free columns */
            for (uint16_t j = 0; j < catalog->tables[i]->num_columns; j++) {
                FREE(catalog->tables[i]->columns[j].default_value);
            }
            FREE(catalog->tables[i]->columns);
            
            /* Free indexes */
            FREE(catalog->tables[i]->indexes);
            
            FREE(catalog->tables[i]);
        }
    }
    FREE(catalog->tables);
    
    /* Free indexes */
    if (catalog->indexes != NULL) {
        for (uint16_t i = 0; i < catalog->num_tables * 4; i++) {
            if (catalog->indexes[i] != NULL) {
                FREE(catalog->indexes[i]->column_positions);
                FREE(catalog->indexes[i]);
            }
        }
        FREE(catalog->indexes);
    }
    
    /* Free table IDs */
    FREE(catalog->database.table_ids);
    
    FREE(catalog);
}

/**
 * @brief Load catalog from storage
 */
int catalog_load(catalog_t *catalog) {
    catalog_header_t header;
    size_t bytes_read;
    
    if (catalog == NULL || catalog->storage == NULL) {
        return -1;
    }
    
    /* Open catalog file */
    catalog->catalog_file = storage_open_file(catalog->storage, "catalog.dat");
    if (catalog->catalog_file == NULL) {
        /* Catalog file doesn't exist yet, create it later */
        return 0;
    }
    
    /* Read header */
    bytes_read = fread(&header, 1, sizeof(header), catalog->catalog_file);
    if (bytes_read != sizeof(header)) {
        fprintf(stderr, "Cannot read catalog header\n");
        return -1;
    }
    
    /* Validate magic number */
    if (header.magic != CATALOG_MAGIC) {
        fprintf(stderr, "Invalid catalog magic number\n");
        return -1;
    }
    
    /* Validate version */
    if (header.version != CATALOG_VERSION) {
        fprintf(stderr, "Unsupported catalog version: %u\n", header.version);
        return -1;
    }
    
    /* Load metadata */
    catalog->version = header.version;
    catalog->last_update = header.last_update;
    
    /* Load database info */
    bytes_read = fread(&catalog->database, 1, sizeof(catalog->database) - sizeof(oid_t *), 
                       catalog->catalog_file);
    if (bytes_read != sizeof(catalog->database) - sizeof(oid_t *)) {
        fprintf(stderr, "Cannot read database info\n");
        return -1;
    }
    
    /* Allocate table IDs array */
    if (catalog->database.num_tables > 0) {
        catalog->database.table_ids = ALLOC(sizeof(oid_t) * catalog->database.num_tables);
        if (catalog->database.table_ids == NULL) {
            return -1;
        }
        bytes_read = fread(catalog->database.table_ids, sizeof(oid_t), 
                           catalog->database.num_tables, catalog->catalog_file);
        if (bytes_read != catalog->database.num_tables) {
            fprintf(stderr, "Cannot read table IDs\n");
            return -1;
        }
    }
    
    /* Load tables */
    catalog->num_tables = header.num_tables;
    if (catalog->num_tables > 0) {
        catalog->tables = ALLOC(sizeof(table_info_t *) * catalog->num_tables);
        if (catalog->tables == NULL) {
            return -1;
        }
        memset(catalog->tables, 0, sizeof(table_info_t *) * catalog->num_tables);
        
        for (uint16_t i = 0; i < catalog->num_tables; i++) {
            catalog->tables[i] = ALLOC_ZERO(sizeof(table_info_t));
            if (catalog->tables[i] == NULL) {
                return -1;
            }
            bytes_read = fread(catalog->tables[i], 1, sizeof(table_info_t) - sizeof(column_info_t *) - sizeof(index_info_t **),
                               catalog->catalog_file);
            if (bytes_read != sizeof(table_info_t) - sizeof(column_info_t *) - sizeof(index_info_t **)) {
                fprintf(stderr, "Cannot read table info\n");
                return -1;
            }
            
            /* Load columns */
            if (catalog->tables[i]->num_columns > 0) {
                catalog->tables[i]->columns = ALLOC(sizeof(column_info_t) * catalog->tables[i]->num_columns);
                if (catalog->tables[i]->columns == NULL) {
                    return -1;
                }
                bytes_read = fread(catalog->tables[i]->columns, sizeof(column_info_t), 
                                   catalog->tables[i]->num_columns, catalog->catalog_file);
                if (bytes_read != catalog->tables[i]->num_columns) {
                    fprintf(stderr, "Cannot read columns\n");
                    return -1;
                }
                
                /* Fix default value pointers (these were serialized as offsets) */
                for (uint16_t j = 0; j < catalog->tables[i]->num_columns; j++) {
                    catalog->tables[i]->columns[j].default_value = NULL;
                }
            }
        }
    }
    
    return 0;
}

/**
 * @brief Save catalog to storage
 */
int catalog_save(catalog_t *catalog) {
    catalog_header_t header;
    
    if (catalog == NULL || catalog->storage == NULL) {
        return -1;
    }
    
    /* Open catalog file */
    if (catalog->catalog_file == NULL) {
        catalog->catalog_file = storage_open_file(catalog->storage, "catalog.dat");
        if (catalog->catalog_file == NULL) {
            return -1;
        }
    }
    
    /* Build header */
    header.magic = CATALOG_MAGIC;
    header.version = CATALOG_VERSION;
    header.last_update = time(NULL);
    header.next_table_id = catalog->num_tables > 0 ? catalog->num_tables + 1 : 1;
    header.next_index_id = catalog->num_tables * 4 + 1;
    header.num_tables = catalog->num_tables;
    header.num_indexes = 0;  /* TODO: Count indexes */
    
    /* Write header */
    if (fwrite(&header, 1, sizeof(header), catalog->catalog_file) != sizeof(header)) {
        fprintf(stderr, "Cannot write catalog header\n");
        return -1;
    }
    
    /* Write database info */
    if (fwrite(&catalog->database, 1, sizeof(catalog->database) - sizeof(oid_t *), 
               catalog->catalog_file) != sizeof(catalog->database) - sizeof(oid_t *)) {
        fprintf(stderr, "Cannot write database info\n");
        return -1;
    }
    
    /* Write table IDs */
    if (catalog->database.table_ids != NULL) {
        if (fwrite(catalog->database.table_ids, sizeof(oid_t), 
                   catalog->database.num_tables, catalog->catalog_file) != catalog->database.num_tables) {
            fprintf(stderr, "Cannot write table IDs\n");
            return -1;
        }
    }
    
    /* Flush to disk */
    fflush(catalog->catalog_file);
    
    return 0;
}

/**
 * @brief Create a new table
 */
oid_t catalog_create_table(catalog_t *catalog, const char *name, 
                           column_info_t *columns, int num_columns) {
    table_info_t *table;
    oid_t table_id;
    
    if (catalog == NULL || name == NULL || columns == NULL || num_columns <= 0) {
        return 0;
    }
    
    /* Allocate new table */
    table = ALLOC_ZERO(sizeof(table_info_t));
    if (table == NULL) {
        return 0;
    }
    
    /* Generate table ID */
    table_id = ++catalog->num_tables;
    
    /* Copy table info */
    table->table_id = table_id;
    strncpy(table->name, name, sizeof(table->name) - 1);
    table->num_columns = num_columns;
    table->num_indexes = 0;
    table->num_rows = 0;
    table->flags = 0;
    table->created_at = time(NULL);
    table->modified_at = time(NULL);
    
    /* Build filename */
    snprintf(table->filename, sizeof(table->filename), "%s.heap", name);
    
    /* Allocate and copy columns */
    table->columns = ALLOC(sizeof(column_info_t) * num_columns);
    if (table->columns == NULL) {
        FREE(table);
        return 0;
    }
    memcpy(table->columns, columns, sizeof(column_info_t) * num_columns);
    
    /* Fix default value pointers */
    for (int i = 0; i < num_columns; i++) {
        if (table->columns[i].default_value != NULL) {
            table->columns[i].default_value = STRDUP(table->columns[i].default_value);
        }
    }
    
    /* Add to tables array */
    table_info_t **new_tables = REALLOC(catalog->tables, sizeof(table_info_t *) * catalog->num_tables);
    if (new_tables == NULL) {
        FREE(table->columns);
        FREE(table);
        return 0;
    }
    catalog->tables = new_tables;
    catalog->tables[catalog->num_tables - 1] = table;
    
    /* Update database */
    oid_t *new_ids = REALLOC(catalog->database.table_ids, sizeof(oid_t) * catalog->num_tables);
    if (new_ids == NULL) {
        /* Rollback */
        catalog->num_tables--;
        FREE(table->columns);
        FREE(table);
        return 0;
    }
    catalog->database.table_ids = new_ids;
    catalog->database.table_ids[catalog->num_tables - 1] = table_id;
    catalog->database.num_tables = catalog->num_tables;
    
    /* Save catalog */
    catalog_save(catalog);
    
    return table_id;
}

/**
 * @brief Drop a table
 */
int catalog_drop_table(catalog_t *catalog, oid_t table_id) {
    if (catalog == NULL || table_id == 0) {
        return -1;
    }
    
    /* Find table */
    for (uint16_t i = 0; i < catalog->num_tables; i++) {
        if (catalog->tables[i] != NULL && catalog->tables[i]->table_id == table_id) {
            /* Free table resources */
            for (uint16_t j = 0; j < catalog->tables[i]->num_columns; j++) {
                FREE(catalog->tables[i]->columns[j].default_value);
            }
            FREE(catalog->tables[i]->columns);
            FREE(catalog->tables[i]->indexes);
            FREE(catalog->tables[i]);
            catalog->tables[i] = NULL;
            
            /* Remove from database table IDs */
            for (uint16_t j = 0; j < catalog->database.num_tables; j++) {
                if (catalog->database.table_ids[j] == table_id) {
                    catalog->database.table_ids[j] = 0;
                    break;
                }
            }
            
            /* Save catalog */
            catalog_save(catalog);
            
            return 0;
        }
    }
    
    return -1;
}

/**
 * @brief Get table information
 */
table_info_t *catalog_get_table(catalog_t *catalog, oid_t table_id) {
    if (catalog == NULL || table_id == 0) {
        return NULL;
    }
    
    for (uint16_t i = 0; i < catalog->num_tables; i++) {
        if (catalog->tables[i] != NULL && catalog->tables[i]->table_id == table_id) {
            return catalog->tables[i];
        }
    }
    
    return NULL;
}

/**
 * @brief Get table by name
 */
table_info_t *catalog_get_table_by_name(catalog_t *catalog, const char *name) {
    if (catalog == NULL || name == NULL) {
        return NULL;
    }
    
    for (uint16_t i = 0; i < catalog->num_tables; i++) {
        if (catalog->tables[i] != NULL && 
            strcmp(catalog->tables[i]->name, name) == 0) {
            return catalog->tables[i];
        }
    }
    
    return NULL;
}

/**
 * @brief Get all tables
 */
table_info_t **catalog_get_all_tables(catalog_t *catalog, int *num_tables) {
    table_info_t **result;
    int count = 0;
    
    if (catalog == NULL || num_tables == NULL) {
        return NULL;
    }
    
    /* Count non-NULL tables */
    for (uint16_t i = 0; i < catalog->num_tables; i++) {
        if (catalog->tables[i] != NULL) {
            count++;
        }
    }
    
    /* Allocate result */
    result = ALLOC(sizeof(table_info_t *) * count);
    if (result == NULL) {
        return NULL;
    }
    
    /* Copy table pointers */
    count = 0;
    for (uint16_t i = 0; i < catalog->num_tables; i++) {
        if (catalog->tables[i] != NULL) {
            result[count++] = catalog->tables[i];
        }
    }
    
    *num_tables = count;
    return result;
}

/**
 * @brief Create an index
 */
oid_t catalog_create_index(catalog_t *catalog, const char *name, oid_t table_id,
                           uint16_t *column_positions, int num_columns,
                           uint8_t index_type, bool is_unique) {
    /* TODO: Implement index creation */
    (void)catalog;
    (void)name;
    (void)table_id;
    (void)column_positions;
    (void)num_columns;
    (void)index_type;
    (void)is_unique;
    
    return 0;
}

/**
 * @brief Drop an index
 */
int catalog_drop_index(catalog_t *catalog, oid_t index_id) {
    (void)catalog;
    (void)index_id;
    
    return -1;
}

/**
 * @brief Get index information
 */
index_info_t *catalog_get_index(catalog_t *catalog, oid_t index_id) {
    (void)catalog;
    (void)index_id;
    
    return NULL;
}

/**
 * @brief Get indexes for a table
 */
index_info_t **catalog_get_table_indexes(catalog_t *catalog, oid_t table_id, int *num_indexes) {
    (void)catalog;
    (void)table_id;
    
    *num_indexes = 0;
    return NULL;
}

/**
 * @brief Update table statistics
 */
int catalog_update_table_stats(catalog_t *catalog, oid_t table_id, uint64_t num_rows) {
    table_info_t *table = catalog_get_table(catalog, table_id);
    
    if (table == NULL) {
        return -1;
    }
    
    table->num_rows = num_rows;
    table->modified_at = time(NULL);
    
    catalog_save(catalog);
    
    return 0;
}

/**
 * @brief Create a new database
 */
int catalog_create_database(catalog_t *catalog, const char *name, 
                           uint32_t encoding, const char *collation) {
    if (catalog == NULL || name == NULL) {
        return -1;
    }
    
    strncpy(catalog->database.name, name, sizeof(catalog->database.name) - 1);
    catalog->database.encoding = encoding;
    if (collation != NULL) {
        strncpy(catalog->database.collation, collation, sizeof(catalog->database.collation) - 1);
    }
    
    return catalog_save(catalog);
}

/**
 * @brief Get database information
 */
database_info_t *catalog_get_database(catalog_t *catalog) {
    if (catalog == NULL) {
        return NULL;
    }
    
    return &catalog->database;
}

/**
 * @brief Check if table exists
 */
bool catalog_table_exists(catalog_t *catalog, oid_t table_id) {
    return catalog_get_table(catalog, table_id) != NULL;
}

/**
 * @brief Check if table exists by name
 */
bool catalog_table_exists_by_name(catalog_t *catalog, const char *name) {
    return catalog_get_table_by_name(catalog, name) != NULL;
}

/**
 * @brief Print catalog contents
 */
void catalog_print(catalog_t *catalog) {
    printf("=== Catalog ===\n");
    printf("Database: %s\n", catalog->database.name);
    printf("Tables: %u\n", catalog->database.num_tables);
    
    for (uint16_t i = 0; i < catalog->num_tables; i++) {
        if (catalog->tables[i] != NULL) {
            printf("  Table: %s (ID: %u, Columns: %u)\n",
                   catalog->tables[i]->name,
                   catalog->tables[i]->table_id,
                   catalog->tables[i]->num_columns);
        }
    }
}

/** @} */ /* catalog */
