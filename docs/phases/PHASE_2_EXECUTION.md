# Phase 2: Execution & Networking

## Overview
Phase 2 focused on transforming raw data into results through a standardized execution model and enabling communication between nodes.

## Key Components

### 1. Volcano Execution Engine (`executor/operator.cpp`)
Implemented the standard pull-based iterator model.
- **Base Operator Class**: Defines the `init`, `open`, `next`, and `close` interface.
- **Physical Operators**:
    - `SeqScanOperator`: Linear scan of heap tables.
    - `FilterOperator`: Expression evaluation using the Value system.
    - `ProjectOperator`: Column transformation and aliasing.
    - `HashJoinOperator`: Efficient in-memory inner joins.

### 2. Internal RPC Layer (`network/rpc_server.cpp`, `rpc_client.cpp`)
Built a high-performance communication backbone for the cluster.
- **Binary Protocol**: Custom header-payload format for minimal overhead.
- **Command Routing**: Registry-based handler system for different RPC types (`ExecuteFragment`, `TxnPrepare`, etc.).
- **Async Execution**: Support for parallel query dispatch to multiple nodes.

### 3. PostgreSQL Wire Protocol (`network/server.cpp`)
Ensured compatibility with standard SQL tools.
- **Handshake**: Support for startup messages and authentication.
- **Simple Query Protocol**: Enables tools like `psql` to send SQL strings and receive formatted results.

### 4. Transaction Management (`transaction/lock_manager.cpp`)
Implemented local concurrency control.
- **Two-Phase Locking (2PL)**: Support for Shared (S) and Exclusive (X) locks.
- **Two-Phase Commit (2PC)**: Infrastructure for distributed transaction coordination (Prepare/Commit/Abort).

## Lessons Learned
- The pull-based model simplifies operator composition but requires careful memory management of intermediate results.
- Sockets with `MSG_WAITALL` require strict protocol adherence to avoid deadlocks.
