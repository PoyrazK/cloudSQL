# cloudSQL

A lightweight, distributed SQL database engine. Designed for cloud environments with a focus on simplicity, type safety, and PostgreSQL compatibility.

## Key Features

- **Modern C++ Architecture**: High-performance, object-oriented codebase using C++17.
- **Type-Safe Value System**: Robust handling of SQL data types (Integer, Float, Text, Boolean, etc.) using `std::variant`.
- **Paged Storage Engine**: Efficient page-level random access I/O via a custom `StorageManager`.
- **Slot-Based Heap Tables**: Optimized row-oriented storage with support for variable-length data.
- **B+ Tree Indexing**: Fast secondary access paths for point lookups and ordered scans.
- **SQL Parser**: Powerful recursive descent parser supporting DDL (`CREATE TABLE`) and DML (`INSERT`, `SELECT` with `WHERE`, `GROUP BY`, `ORDER BY`, `LIMIT`).
- **Volcano Execution Engine**: Advanced iterator-based execution supporting sequential scans, index scans, filtering, projection, hash joins, sorting, and aggregation.
- **PostgreSQL Wire Protocol**: Handshake and simple query protocol implementation for tool compatibility.

## Project Structure

- `include/`: Header files defining the core engine API.
- `src/`: Core implementation modules.
  - `catalog/`: Metadata and schema management.
  - `common/`: Core types and configuration.
  - `executor/`: Query operators and execution coordination.
  - `network/`: PostgreSQL server implementation.
  - `parser/`: Lexical analysis and SQL parsing.
  - `storage/`: Paged storage, heap files, and indexes.
- `tests/`: Comprehensive test suite for reliability and performance.

## Building and Running

### Prerequisites

- CMake (>= 3.16)
- C++17 compatible compiler (Clang or GCC)

### Build Instructions

```bash
mkdir build
cd build
cmake ..
make
```

### Running Tests

```bash
./build/sqlEngine_tests
```

### Starting the Server

```bash
./build/sqlEngine --port 5432 --data ./data
```

## Core Components

### 1. Value System
The engine features a unified `Value` class that safely encapsulates SQL types. This ensures data integrity during calculations and data retrieval.

### 2. Execution Operators
Queries are executed using the Volcano model, allowing for scalable and modular operator trees:
- `SeqScanOperator`: Scans all tuples in a table.
- `IndexScanOperator`: Leverages B+ Trees for high-speed lookups.
- `FilterOperator`: Efficiently filters data based on complex expressions.
- `ProjectOperator`: Computes results and transforms data columns.
- `SortOperator`: Handles `ORDER BY` with multiple keys and directions.
- `AggregateOperator`: Implements `GROUP BY` and aggregate functions (`COUNT`, `SUM`, etc.).
- `HashJoinOperator`: Performs high-performance in-memory inner joins.
- `LimitOperator`: Manages result set windowing.

### 3. Storage Layer
Data is persisted in fixed-size pages (default 4KB) using a slot-based layout. The `StorageManager` coordinates access to these pages, ensuring atomic operations and enabling future support for buffer pool management.

## License

MIT
