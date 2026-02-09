/**
 * @file common.h
 * @brief Common macros and utilities for the SQL Engine
 */

#ifndef SQL_ENGINE_COMMON_H
#define SQL_ENGINE_COMMON_H

#include <stddef.h>
#include <stdlib.h>

/* Memory allocation macros */
#define MALLOC(size) malloc(size)
#define ALLOC(size) malloc(size)
#define ALLOC_ZERO(size) calloc(1, size)
#define REALLOC(ptr, size) realloc(ptr, size)
#define FREE(ptr) do { \
    if ((ptr) != NULL) { \
        free(ptr); \
        (ptr) = NULL; \
    } \
} while (0)

/* String utilities */
#define STRDUP(str) ((str) ? strdup(str) : NULL)

/* Assert macro */
#ifndef NDEBUG
#include <assert.h>
#define SQL_ASSERT(cond) assert(cond)
#else
#define SQL_ASSERT(cond) ((void)0)
#endif

/* Min/Max macros */
#define SQL_MIN(a, b) ((a) < (b) ? (a) : (b))
#define SQL_MAX(a, b) ((a) > (b) ? (a) : (b))

/* Offset of a field in a struct */
#define SQL_OFFSETOF(struct_type, field) ((size_t)&(((struct_type *)0)->field))

#endif /* SQL_ENGINE_COMMON_H */
