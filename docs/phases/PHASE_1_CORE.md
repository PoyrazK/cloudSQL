# Phase 1: Core Foundation

## Overview
Phase 1 established the fundamental types and storage primitives required for a modern, type-safe SQL engine.

## Key Components

### 1. Unified Value System (`common/value.hpp`)
Transitioned from C-style unions to `std::variant`.
- **Type Safety**: Use of `std::get` and `std::holds_alternative` prevents invalid memory access.
- **Null Handling**: Explicit `std::monostate` representation for SQL `NULL`.
- **Operators**: Overloaded comparison and arithmetic operators for native SQL expression evaluation.

### 2. Paged Storage Manager (`storage/storage_manager.cpp`)
Implemented a platform-agnostic abstraction for random access I/O.
- **Fixed-size Pages**: Default 4KB pages matching OS memory pages.
- **Atomic Operations**: Ensure consistent page-level reads and writes.

### 3. Buffer Pool Manager (`storage/buffer_pool_manager.cpp`)
Introduced a caching layer to minimize disk I/O.
- **Replacement Policy**: LRU-K algorithm implementation for intelligent page eviction.
- **Thread Safety**: Mutex-guarded page table and free list management.
- **Pinning**: Support for pinning pages in memory during critical operations.

### 4. Slot-based Heap Tables (`storage/heap_table.cpp`)
Implemented the physical row storage format.
- **Slotted Pages**: Header-based layout tracking row offsets and lengths.
- **Variable Length Support**: Efficient handling of `VARCHAR` and `TEXT` data.
- **Meta-data Management**: In-page tracking of `xmin`, `xmax`, and `lsn` for MVCC and recovery.

## Lessons Learned
- Pre-allocating the buffer pool reduces runtime fragmentation.
- Binary compatibility with the previous C implementation was maintained for initial data migration.
