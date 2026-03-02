### Description
This PR completes Phase 5 of the cloudSQL C++ migration and distributed optimization roadmap. It introduces key performance enhancements for cross-node queries, robust data redistribution infrastructure, and advanced join orchestration.

### Key Changes
- **Distributed Query Optimization**:
  - Implemented **Shard Pruning** to intelligently route point queries to specific shards, reducing network traffic.
  - Added **Aggregation Merging** logic to the `DistributedExecutor` to coordinate global `COUNT` and `SUM` operations.
  - Introduced **Broadcast Join** orchestration, enabling efficient joins between small and large tables by redistributing data across the cluster.
- **Execution Infrastructure**:
  - Added `BufferScanOperator` to allow the Volcano engine to scan in-memory shuffle buffers.
  - Integrated `ClusterManager` buffering for staging redistributed data received via RPC.
- **Networking & Serialization**:
  - Implemented comprehensive `Value` and `Tuple` binary serialization in `rpc_message.hpp`.
  - Hardened the RPC layer with `MSG_WAITALL` and robust read patterns to prevent synchronization hangs.
- **Parser Improvements**:
  - Enhanced `INSERT` parsing for multi-row values.
  - Added support for `COUNT(*)` and improved function expression handling.
- **Documentation**:
  - Created a detailed technical record for all 5 phases in `docs/phases/`.
  - Updated `README.md` and `architecture.md` to reflect the new distributed capabilities.

### Validation Results
- **Core Tests**: 28/28 passing.
- **Distributed Tests**: 6/6 passing (covering Shard Pruning, Shuffle, and Broadcast Join).
- **Transaction Tests**: 3/3 passing (2PC stability verified).
- **Build**: Clean build on AppleClang 17.0.0.

### Steps to Test
1. Build the project: `mkdir build && cd build && cmake .. && make -j`
2. Run unit tests: `./sqlEngine_tests`
3. Run distributed tests: `./distributed_tests`
4. Run transaction tests: `./distributed_txn_tests`
