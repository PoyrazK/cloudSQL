/**
 * @file config.c
 * @brief Configuration implementation
 *
 * @defgroup config Configuration
 * @{
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h>

#include "common/config.h"

/* Default configuration */
const config_t CONFIG_DEFAULT = {
    .port = DEFAULT_PORT,
    .data_dir = DEFAULT_DATA_DIR,
    .config_file = "",
    .mode = MODE_EMBEDDED,
    .max_connections = DEFAULT_MAX_CONNECTIONS,
    .buffer_pool_size = DEFAULT_BUFFER_POOL_SIZE,
    .page_size = DEFAULT_PAGE_SIZE,
    .debug = false,
    .verbose = false
};

/**
 * @brief Trim whitespace from string
 */
static char *trim(char *str) {
    char *end;
    
    /* Trim leading whitespace */
    while (*str == ' ' || *str == '\t' || *str == '\n' || *str == '\r') {
        str++;
    }
    
    /* Trim trailing whitespace */
    end = str + strlen(str) - 1;
    while (end > str && (*end == ' ' || *end == '\t' || *end == '\n' || *end == '\r')) {
        end--;
    }
    *(end + 1) = '\0';
    
    return str;
}

/**
 * @brief Initialize configuration with defaults
 */
void config_init(config_t *config) {
    *config = CONFIG_DEFAULT;
}

/**
 * @brief Load configuration from file
 */
int config_load(config_t *config, const char *filename) {
    FILE *fp;
    char line[512];
    char *key, *value;
    
    if (filename == NULL || strlen(filename) == 0) {
        return -1;
    }
    
    fp = fopen(filename, "r");
    if (fp == NULL) {
        fprintf(stderr, "Cannot open config file: %s\n", filename);
        return -1;
    }
    
    while (fgets(line, sizeof(line), fp) != NULL) {
        /* Skip empty lines and comments */
        if (line[0] == '#' || line[0] == '\n' || line[0] == '\r') {
            continue;
        }
        
        /* Parse key=value */
        key = strtok(line, "=");
        value = strtok(NULL, "\n");
        
        if (key == NULL || value == NULL) {
            continue;
        }
        
        key = trim(key);
        value = trim(value);
        
        /* Parse configuration options */
        if (strcmp(key, "port") == 0) {
            config->port = (uint16_t)atoi(value);
        } else if (strcmp(key, "data_dir") == 0) {
            strncpy(config->data_dir, value, sizeof(config->data_dir) - 1);
        } else if (strcmp(key, "max_connections") == 0) {
            config->max_connections = atoi(value);
        } else if (strcmp(key, "buffer_pool_size") == 0) {
            config->buffer_pool_size = atoi(value);
        } else if (strcmp(key, "page_size") == 0) {
            config->page_size = atoi(value);
        } else if (strcmp(key, "mode") == 0) {
            if (strcmp(value, "distributed") == 0) {
                config->mode = MODE_DISTRIBUTED;
            } else {
                config->mode = MODE_EMBEDDED;
            }
        } else if (strcmp(key, "debug") == 0) {
            config->debug = (strcmp(value, "true") == 0 || strcmp(value, "1") == 0);
        } else if (strcmp(key, "verbose") == 0) {
            config->verbose = (strcmp(value, "true") == 0 || strcmp(value, "1") == 0);
        }
    }
    
    fclose(fp);
    return 0;
}

/**
 * @brief Save configuration to file
 */
int config_save(config_t *config, const char *filename) {
    FILE *fp;
    
    if (filename == NULL) {
        return -1;
    }
    
    fp = fopen(filename, "w");
    if (fp == NULL) {
        fprintf(stderr, "Cannot open config file for writing: %s\n", filename);
        return -1;
    }
    
    fprintf(fp, "# SQL Engine Configuration\n");
    fprintf(fp, "# Auto-generated\n\n");
    
    fprintf(fp, "port=%d\n", config->port);
    fprintf(fp, "data_dir=%s\n", config->data_dir);
    fprintf(fp, "max_connections=%d\n", config->max_connections);
    fprintf(fp, "buffer_pool_size=%d\n", config->buffer_pool_size);
    fprintf(fp, "page_size=%d\n", config->page_size);
    fprintf(fp, "mode=%s\n", config->mode == MODE_DISTRIBUTED ? "distributed" : "embedded");
    fprintf(fp, "debug=%s\n", config->debug ? "true" : "false");
    fprintf(fp, "verbose=%s\n", config->verbose ? "true" : "false");
    
    fclose(fp);
    return 0;
}

/**
 * @brief Validate configuration
 */
int config_validate(config_t *config) {
    if (config->port == 0 || config->port > 65535) {
        fprintf(stderr, "Invalid port number: %d\n", config->port);
        return -1;
    }
    
    if (config->max_connections < 1) {
        fprintf(stderr, "Invalid max connections: %d\n", config->max_connections);
        return -1;
    }
    
    if (config->buffer_pool_size < 1) {
        fprintf(stderr, "Invalid buffer pool size: %d\n", config->buffer_pool_size);
        return -1;
    }
    
    if (config->page_size < 1024 || config->page_size > 65536) {
        fprintf(stderr, "Invalid page size: %d (must be between 1024 and 65536)\n", config->page_size);
        return -1;
    }
    
    if (strlen(config->data_dir) == 0) {
        fprintf(stderr, "Data directory cannot be empty\n");
        return -1;
    }
    
    return 0;
}

/**
 * @brief Print configuration to stdout
 */
void config_print(config_t *config) {
    printf("=== SQL Engine Configuration ===\n");
    printf("Version:      %s\n", SQL_ENGINE_VERSION);
    printf("Mode:         %s\n", config->mode == MODE_DISTRIBUTED ? "distributed" : "embedded");
    printf("Port:         %d\n", config->port);
    printf("Data dir:     %s\n", config->data_dir);
    printf("Max conns:    %d\n", config->max_connections);
    printf("Buffer pool:  %d pages\n", config->buffer_pool_size);
    printf("Page size:    %d bytes\n", config->page_size);
    printf("Debug:        %s\n", config->debug ? "enabled" : "disabled");
    printf("Verbose:      %s\n", config->verbose ? "enabled" : "disabled");
    printf("================================\n");
}

/** @} */ /* config */
