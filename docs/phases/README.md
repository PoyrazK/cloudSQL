# cloudSQL C++ Migration & Distributed Roadmap

This directory contains the technical documentation for the lifecycle of the cloudSQL migration from C to C++, and its subsequent expansion into a distributed engine.

## Lifecycle Phases

### [Phase 1: Core Foundation](./PHASE_1_CORE.md)
**Focus**: Type safety and Paged Storage.
- Modernized `Value` system using `std::variant`.
- Binary-compatible `StorageManager` and thread-safe `BufferPoolManager`.
- Slot-based `HeapTable` implementation.

### [Phase 2: Execution & Networking](./PHASE_2_EXECUTION.md)
**Focus**: Volcano Model & Communication.
- Iterator-based physical operators (`SeqScan`, `Filter`, `Project`, `HashJoin`).
- POSIX-based internal RPC layer.
- PostgreSQL Wire Protocol (Handshake + Simple Query).
- Local `LockManager` for concurrency control.

### [Phase 3: SQL & Catalog](./PHASE_3_SQL_CATALOG.md)
**Focus**: SQL Ingestion & Metadata.
- Recursive Descent Parser for DDL and DML.
- Global `Catalog` for schema management.
- Integration of System Tables for persistence.

### [Phase 4: Distributed State](./PHASE_4_CONSENSUS.md)
**Focus**: Raft Consistency.
- Core Raft implementation (Leader Election, Heartbeats, Replication).
- Catalog-Raft integration for consistent metadata.
- `ClusterManager` for node discovery and membership.

### [Phase 5: Distributed Optimization](./PHASE_5_OPTIMIZATION.md)
**Focus**: Performance & Advanced Advanced Joins.
- Shard Pruning logic for targeted routing.
- Global Aggregation Merging (COUNT/SUM).
- Broadcast Join orchestration.
- Inter-node data redistribution (Shuffle infrastructure).

---

## Technical Standards
- **Standard**: C++17
- **Build System**: CMake
- **Tests**: GoogleTest
- **Protocol**: Binary internal RPC / PostgreSQL Wire Protocol external.
