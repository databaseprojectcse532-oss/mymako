# Distributed Sequencing Implementation Plan for Mako

## Executive Summary

This document outlines a detailed plan to implement **deterministic distributed sequencing** inspired by Calvin (SIGMOD'12) in the Mako distributed transaction system. The sequencing layer will enable deterministic execution across replicas while maintaining high throughput through batching and pipelining.

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Core Components](#core-components)
3. [Implementation Phases](#implementation-phases)
4. [Detailed Component Design](#detailed-component-design)
5. [Execution Flow with Diagrams](#execution-flow-with-diagrams)
6. [Integration with Existing Mako](#integration-with-existing-mako)
7. [Performance Considerations](#performance-considerations)
8. [Testing Strategy](#testing-strategy)

---

## Architecture Overview

### High-Level Design

The distributed sequencing architecture consists of three layers:

```
┌─────────────────────────────────────────────────────────────┐
│                     CLIENT LAYER                             │
│  - Submit transactions with read/write sets                  │
│  - Receive commit/abort responses                            │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  SEQUENCING LAYER                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │  Sequencer   │  │  Sequencer   │  │  Sequencer   │      │
│  │  Replica 1   │  │  Replica 2   │  │  Replica 3   │      │
│  │  (Leader)    │  │  (Follower)  │  │  (Follower)  │      │
│  └──────────────┘  └──────────────┘  └──────────────┘      │
│         │                 │                 │                │
│         └────── Paxos Replication ──────────┘                │
│                                                               │
│  - Batch transactions into epochs                            │
│  - Assign global sequence numbers                            │
│  - Replicate sequence via Paxos                              │
└─────────────────────────────────────────────────────────────┘
                            ▼
┌─────────────────────────────────────────────────────────────┐
│                  EXECUTION LAYER                             │
│  ┌─────────────────┐  ┌─────────────────┐                   │
│  │  Shard 1        │  │  Shard 2        │  ...              │
│  │  ┌───────────┐  │  │  ┌───────────┐  │                   │
│  │  │ Replica A │  │  │  │ Replica A │  │                   │
│  │  ├───────────┤  │  │  ├───────────┤  │                   │
│  │  │ Replica B │  │  │  │ Replica B │  │                   │
│  │  ├───────────┤  │  │  ├───────────┤  │                   │
│  │  │ Replica C │  │  │  │ Replica C │  │                   │
│  │  └───────────┘  │  │  └───────────┘  │                   │
│  └─────────────────┘  └─────────────────┘                   │
│                                                               │
│  - Execute transactions in sequence order                    │
│  - Acquire locks deterministically                           │
│  - Active replication (all replicas execute)                 │
└─────────────────────────────────────────────────────────────┘
```

### Key Principles

1. **Deterministic Execution**: All replicas execute transactions in the exact same order
2. **Sequencing Before Execution**: Global order is established before any execution
3. **Batching for Throughput**: Transactions batched into epochs (e.g., 10ms windows)
4. **Paxos for Consensus**: Multi-Paxos ensures all sequencers agree on batch order
5. **Dependency Awareness**: Cross-shard transactions coordinated via sequence numbers

---

## Core Components

### 1. **Sequencer Service**

**Responsibility**: Batch and order transactions globally

**Key Data Structures**:
```
struct SequencerBatch {
    uint64_t epoch_id;              // Monotonically increasing epoch
    uint64_t paxos_slot;            // Paxos slot for this batch
    vector<Transaction> txns;       // Transactions in this batch
    uint64_t base_sequence_number;  // Starting sequence number
    timestamp_t batch_timestamp;    // When batch was created
};

class Sequencer {
    // Batching state
    SequencerBatch* current_batch_;
    uint64_t current_epoch_id_;
    uint64_t next_sequence_number_;

    // Paxos coordination
    MultiPaxosCoordinator* paxos_coordinator_;
    uint64_t current_paxos_slot_;

    // Timing
    Timer batch_timer_;
    uint64_t batch_interval_ms_;  // e.g., 10ms

    // Communication
    SequencerCommunicator* comm_;
};
```

**Main Operations**:
- `SubmitTransaction(txn)`: Add txn to current batch
- `SealBatch()`: Close current batch and start Paxos
- `OnBatchCommitted(batch)`: Send batch to execution layer
- `OnBatchTimeout()`: Seal batch after time window

### 2. **Paxos Coordinator for Sequencing**

**Responsibility**: Replicate batch order across sequencer replicas

**Key Data Structures**:
```
struct PaxosInstance {
    uint64_t slot_id;
    SequencerBatch* proposed_batch;
    uint64_t ballot_number;
    PaxosPhase phase;  // PREPARE, ACCEPT, COMMIT
    set<int> accept_acks;
};

class SequencerPaxos : public MultiPaxosCoordinator {
    map<uint64_t, PaxosInstance> active_instances_;
    uint64_t min_uncommitted_slot_;
    uint64_t max_committed_slot_;
    int leader_id_;
};
```

**Paxos Flow**:
```
Leader Sequencer                   Follower Sequencers
     │                                    │
     │ 1. Seal batch at time T            │
     │                                    │
     │ 2. Prepare(slot, ballot)           │
     ├───────────────────────────────────→│
     │                                    │
     │ 3. Promise(slot, ballot)           │
     │←───────────────────────────────────┤
     │                                    │
     │ 4. Accept(slot, ballot, batch)     │
     ├───────────────────────────────────→│
     │                                    │
     │ 5. Accepted(slot, ballot)          │
     │←───────────────────────────────────┤
     │                                    │
     │ 6. Commit(slot, batch)             │
     ├───────────────────────────────────→│
     │                                    │
     │ 7. Forward batch to shards         │
     ├───────────────────────────────────→│ (Both send to execution layer)
```

### 3. **Execution Scheduler**

**Responsibility**: Execute transactions in sequence order

**Key Data Structures**:
```
struct SequencedTransaction {
    uint64_t sequence_number;
    uint64_t epoch_id;
    Transaction txn;
    ExecutionStatus status;  // PENDING, LOCKED, EXECUTING, COMMITTED
};

class DeterministicScheduler {
    // Ordering state
    priority_queue<SequencedTransaction> pending_queue_;
    uint64_t next_expected_sequence_;
    uint64_t last_executed_sequence_;

    // Lock management
    map<string, LockQueue> lock_queues_;  // key -> queue of txns

    // Replication
    int replica_id_;
    vector<ReplicaPeer*> other_replicas_;
};
```

**Main Operations**:
- `ReceiveBatch(batch)`: Add sequenced transactions to pending queue
- `ExecuteNext()`: Execute next transaction in sequence order
- `AcquireLocks(txn)`: Deterministically acquire locks
- `ReleaseLocks(txn)`: Release locks after commit

### 4. **Transaction Metadata**

**Extended Transaction Structure**:
```
struct SequencedTxn {
    // Original transaction
    txn_id_t original_txn_id;
    vector<TxnPiece> pieces;
    set<int> participating_shards;

    // Sequencing metadata
    uint64_t sequence_number;    // Global sequence number
    uint64_t epoch_id;           // Which batch it belongs to
    timestamp_t sequence_time;   // When it was sequenced

    // Execution tracking
    ExecutionPhase phase;        // SEQUENCED, LOCKING, EXECUTING, COMMITTING
    set<int> shards_completed;   // Which shards finished

    // Dependency metadata (optional optimization)
    set<string> read_set_keys;
    set<string> write_set_keys;
};
```

---

## Implementation Phases

### Phase 1: Foundation (Weeks 1-2)

**Goal**: Build basic sequencing infrastructure without Paxos

**Tasks**:
1. Create `Sequencer` class with basic batching logic
2. Implement `SequencerBatch` data structure
3. Add timer-based batch sealing (10ms windows)
4. Create `SequencedTransaction` wrapper
5. Implement single-sequencer (no replication) version
6. Add RPC interface: `SubmitToSequencer()`, `ReceiveBatch()`

**Deliverable**: Single sequencer can batch and forward transactions

**Testing**:
- Unit tests for batching logic
- Simple integration test: Client → Sequencer → Shard

---

### Phase 2: Paxos Integration (Weeks 3-4)

**Goal**: Replicate batch order using Multi-Paxos

**Tasks**:
1. Extend existing `MultiPaxosCoordinator` for sequencer use
2. Implement `SequencerPaxos` class
3. Add Paxos log for committed batches
4. Implement leader election for sequencer
5. Add follower batch reception and forwarding
6. Implement gap detection and recovery

**Deliverable**: 3+ sequencer replicas agree on batch order

**Testing**:
- Paxos unit tests with 3/5 replicas
- Leader failure and election test
- Batch order consistency across replicas

---

### Phase 3: Deterministic Execution (Weeks 5-6)

**Goal**: Execute transactions in sequence order on shards

**Tasks**:
1. Create `DeterministicScheduler` class
2. Implement sequence-order execution queue
3. Add deterministic lock acquisition (by sequence number)
4. Implement per-shard execution tracking
5. Add cross-shard coordination (all shards execute in same order)
6. Handle transaction abort and retry logic

**Deliverable**: Shards execute transactions in exact sequence order

**Testing**:
- Deterministic execution tests (same input → same output)
- Cross-shard transaction tests
- Lock ordering verification

---

### Phase 4: Active Replication (Weeks 7-8)

**Goal**: All replicas execute same sequence

**Tasks**:
1. Add replica group management per shard
2. Implement synchronized execution across replicas
3. Add replica state verification (periodic checksums)
4. Implement replica recovery (catch-up from leader)
5. Add replica failure detection
6. Optimize: leader handles reads, followers replicate writes

**Deliverable**: Shard replicas execute transactions identically

**Testing**:
- State consistency tests across replicas
- Replica failure and recovery tests
- Checksum verification tests

---

### Phase 5: Performance Optimization (Weeks 9-10)

**Goal**: Achieve high throughput via pipelining

**Tasks**:
1. Implement batch pipelining (sequence epoch N while executing N-1)
2. Add dependency analysis for early lock acquisition
3. Optimize: Reconstitution transactions (reads before sequence)
4. Add speculative execution for read-only transactions
5. Implement adaptive batching (adjust window based on load)
6. Add metrics and monitoring

**Deliverable**: System achieves >100K TPS

**Testing**:
- Throughput benchmarks (TPC-C, YCSB)
- Latency measurements (p50, p99)
- Scalability tests (vary # shards, replicas)

---

### Phase 6: Integration & Hardening (Weeks 11-12)

**Goal**: Production-ready system

**Tasks**:
1. Integrate with existing Mako protocols (hybrid mode)
2. Add configuration options (batch size, timeout, Paxos quorum)
3. Implement proper error handling and logging
4. Add operational tools (status checking, manual failover)
5. Performance tuning and profiling
6. Documentation and examples

**Deliverable**: Production-ready distributed sequencing

**Testing**:
- Full system tests with all protocols
- Chaos testing (network partitions, crashes)
- Long-running stability tests

---

## Detailed Component Design

### Sequencer Batching Algorithm

```
┌─────────────────────────────────────────────────────────────┐
│                   BATCHING TIMELINE                          │
└─────────────────────────────────────────────────────────────┘

Epoch 1          Epoch 2          Epoch 3
├────────────┤   ├────────────┤   ├────────────┤
│  Collect   │   │  Collect   │   │  Collect   │
│   Txns     │   │   Txns     │   │   Txns     │
└────┬───────┘   └────┬───────┘   └────┬───────┘
     │                │                │
     ▼                ▼                ▼
 Seal & Paxos    Seal & Paxos    Seal & Paxos
     │                │                │
     ▼                ▼                ▼
  Forward          Forward          Forward
  to Shards        to Shards        to Shards

Time: 0ms    10ms   20ms   30ms   40ms   50ms   60ms
      │       │      │      │      │      │      │
      └───────┴──────┴──────┴──────┴──────┴──────┘
```

**Batching Logic**:

```
on_client_submit(txn):
    1. Add txn to current_batch.txns
    2. If current_batch is full (e.g., 10,000 txns):
        seal_batch()
    3. If batch_timer expires (e.g., 10ms):
        seal_batch()

seal_batch():
    1. batch = current_batch
    2. batch.epoch_id = current_epoch_id++
    3. batch.base_sequence_number = next_sequence_number
    4. next_sequence_number += batch.txns.size()
    5. current_batch = new SequencerBatch()
    6. batch_timer.reset(10ms)
    7. paxos_coordinator.propose(batch)

on_paxos_commit(slot, batch):
    1. Assign sequence numbers:
        for i, txn in enumerate(batch.txns):
            txn.sequence_number = batch.base_sequence_number + i
    2. Forward batch to all shards:
        for shard in all_shards:
            shard.receive_batch(batch)
```

---

### Paxos Sequencing Protocol

```
┌─────────────────────────────────────────────────────────────┐
│           PAXOS REPLICATION OF BATCH ORDER                   │
└─────────────────────────────────────────────────────────────┘

Sequencer 1 (Leader)  Sequencer 2      Sequencer 3
     │                     │                 │
     │ Batch sealed        │                 │
     │ at T=10ms           │                 │
     │                     │                 │
     │ PREPARE(slot=5)     │                 │
     ├────────────────────→│                 │
     ├─────────────────────┼────────────────→│
     │                     │                 │
     │ PROMISE(slot=5)     │                 │
     │←────────────────────┤                 │
     │←────────────────────┼─────────────────┤
     │                     │                 │
     │ [Quorum: 2/3]       │                 │
     │                     │                 │
     │ ACCEPT(slot=5,      │                 │
     │        batch_data)  │                 │
     ├────────────────────→│                 │
     ├─────────────────────┼────────────────→│
     │                     │                 │
     │                     │ Log batch       │
     │                     │                 │ Log batch
     │                     │                 │
     │ ACCEPTED(slot=5)    │                 │
     │←────────────────────┤                 │
     │←────────────────────┼─────────────────┤
     │                     │                 │
     │ [Quorum: 2/3]       │                 │
     │                     │                 │
     │ COMMIT(slot=5)      │                 │
     ├────────────────────→│                 │
     ├─────────────────────┼────────────────→│
     │                     │                 │
     │ Both forward to shards                │
     ├───────────────────────────────────────→ Shard 1
     ├───────────────────────────────────────→ Shard 2
     │                     ├─────────────────→ Shard 1
     │                     ├─────────────────→ Shard 2
```

**Optimization: Multi-Paxos Leader Lease**

Instead of running full Paxos for each batch, use leader lease:

```
Leader holds lease (e.g., 1 second)
    │
    ├─ Epoch 1: Just send ACCEPT (skip PREPARE)
    ├─ Epoch 2: Just send ACCEPT
    ├─ Epoch 3: Just send ACCEPT
    │   ...
    │
    └─ Lease expires → Run full Paxos for next batch
```

This reduces latency from 2 RTTs to 1 RTT per batch.

---

### Deterministic Lock Acquisition

```
┌─────────────────────────────────────────────────────────────┐
│         DETERMINISTIC LOCKING BY SEQUENCE NUMBER            │
└─────────────────────────────────────────────────────────────┘

Lock on key "X":

Sequence  Transaction    Status         Lock Queue for "X"
────────  ───────────    ──────         ──────────────────
  100     T1 (read X)    EXECUTING      [T1: granted]
  101     T2 (write X)   WAITING        [T1: granted, T2: waiting]
  102     T3 (write X)   WAITING        [T1: granted, T2: waiting, T3: waiting]
  103     T4 (read Y)    EXECUTING      (not in queue)

When T1 commits/aborts:
  - Release lock on X
  - Grant to T2 (next in queue by sequence number)

When T2 commits/aborts:
  - Release lock on X
  - Grant to T3 (next in queue)
```

**Locking Algorithm**:

```
execute_transaction(txn):
    1. Sort all locks needed by txn (by key name for determinism)
    2. For each lock in sorted order:
        if lock is free:
            acquire_lock(txn, key)
        else:
            add txn to lock_queues[key] (in sequence order)
            wait_for_lock(txn, key)
    3. Once all locks acquired:
        execute_transaction_logic(txn)
    4. On commit/abort:
        for each key in txn.locks:
            release_lock(key)
            grant_next_in_queue(key)
```

**Key Property**: All replicas acquire locks in the same order because:
- They receive the same batch order (via Paxos)
- They execute transactions in sequence number order
- Lock acquisition is deterministic (sorted by key)

---

### Cross-Shard Transaction Coordination

```
┌─────────────────────────────────────────────────────────────┐
│       CROSS-SHARD TRANSACTION EXECUTION                      │
└─────────────────────────────────────────────────────────────┘

Transaction T (seq=500) accesses Shard 1 and Shard 2

Sequencer                 Shard 1               Shard 2
    │                        │                     │
    │ Batch (seq 500-599)    │                     │
    ├───────────────────────→│                     │
    ├────────────────────────┼────────────────────→│
    │                        │                     │
    │                        │ Execute T at seq=500│
    │                        │ - Acquire locks     │ Execute T at seq=500
    │                        │ - Execute pieces    │ - Acquire locks
    │                        │                     │ - Execute pieces
    │                        │                     │
    │                        │ Send completion msg │
    │                        ├────────────────────→│
    │                        │                     │
    │                        │                     │ Both done?
    │                        │                     │ → Commit
    │                        │                     │
    │                        │←────────────────────┤
    │                        │  Commit ACK         │
    │                        │                     │
    │                        │ Release locks       │ Release locks
    │                        │ Move to seq=501     │ Move to seq=501
```

**Coordination Protocol**:

1. **Sequencer assigns global sequence number** to transaction
2. **Each participating shard**:
   - Receives batch with transaction
   - Waits until sequence number is reached
   - Executes transaction pieces locally
   - Sends completion status to coordinator shard
3. **Coordinator shard** (e.g., shard with first piece):
   - Collects status from all participating shards
   - Makes commit/abort decision
   - Broadcasts decision
4. **All shards commit/abort together**

**No 2PC needed!** Because:
- All shards execute deterministically
- If any shard aborts, all shards will abort
- Deterministic execution ensures same outcome

---

## Execution Flow with Diagrams

### End-to-End Transaction Flow

```
┌────────────────────────────────────────────────────────────────────┐
│                    TRANSACTION LIFECYCLE                            │
└────────────────────────────────────────────────────────────────────┘

Client                Sequencer          Paxos           Shards
  │                       │                │               │
  │ 1. Submit txn         │                │               │
  ├──────────────────────→│                │               │
  │   (read/write sets)   │                │               │
  │                       │                │               │
  │                       │ 2. Add to batch│               │
  │                       │    (wait ~10ms)│               │
  │                       │                │               │
  │                       │ 3. Seal batch  │               │
  │                       │                │               │
  │                       │ 4. Propose     │               │
  │                       ├───────────────→│               │
  │                       │    batch       │               │
  │                       │                │               │
  │                       │                │ 5. Replicate  │
  │                       │                │    (Paxos)    │
  │                       │                │               │
  │                       │ 6. Committed   │               │
  │                       │←───────────────┤               │
  │                       │                │               │
  │                       │ 7. Forward batch              │
  │                       ├───────────────────────────────→│
  │                       │    with sequence #s           │
  │                       │                               │
  │                       │                               │ 8. Queue by
  │                       │                               │    sequence #
  │                       │                               │
  │                       │                               │ 9. Execute in
  │                       │                               │    order
  │                       │                               │
  │                       │                               │ 10. Acquire
  │                       │                               │     locks
  │                       │                               │
  │                       │                               │ 11. Execute
  │                       │                               │     pieces
  │                       │                               │
  │                       │                               │ 12. Commit
  │                       │                               │
  │ 13. Result            │                               │
  │←──────────────────────┼───────────────────────────────┤
  │                       │                               │
```

**Latency Breakdown**:
- Step 1-2: Client → Sequencer (1 RTT) = ~0.5ms
- Step 2-3: Batching wait (average half window) = ~5ms
- Step 4-6: Paxos replication (1 RTT with leader lease) = ~1ms
- Step 7-8: Forward to shards (1 RTT) = ~0.5ms
- Step 9-12: Execution (depends on contention) = ~1-10ms
- Step 13: Response to client (1 RTT) = ~0.5ms

**Total latency**: ~8.5ms (mostly batching wait)

---

### Batch Pipelining for Throughput

```
┌────────────────────────────────────────────────────────────────────┐
│                    PIPELINED BATCH PROCESSING                       │
└────────────────────────────────────────────────────────────────────┘

Time    Sequencer           Paxos             Shards
─────   ─────────────       ──────────        ───────────────

0ms     Epoch 1: Collect
10ms    Epoch 1: Seal
        Epoch 2: Collect    Epoch 1: Paxos
20ms    Epoch 2: Seal       Epoch 1: Commit   Epoch 1: Execute
        Epoch 3: Collect    Epoch 2: Paxos
30ms    Epoch 3: Seal       Epoch 2: Commit   Epoch 1: Execute
        Epoch 4: Collect    Epoch 3: Paxos    Epoch 2: Execute
40ms    Epoch 4: Seal       Epoch 3: Commit   Epoch 1: Finish
        Epoch 5: Collect    Epoch 4: Paxos    Epoch 2: Execute
                                               Epoch 3: Execute
50ms    ...                 ...               ...

        ◄──────────────────────────────────────────────────►
               Steady state: 3 epochs in flight

        Throughput = Batch size / 10ms window
                   = 10,000 txns / 10ms
                   = 1,000,000 TPS
```

**Pipeline Stages**:

1. **Collection**: Sequencer accumulates transactions (10ms window)
2. **Paxos Replication**: Achieve consensus on batch order (1ms)
3. **Execution**: Shards execute transactions (varies, ~10-20ms)

**Concurrency**: Multiple epochs in flight simultaneously

---

### Read-Only Transaction Optimization

```
┌────────────────────────────────────────────────────────────────────┐
│              READ-ONLY TRANSACTION FAST PATH                        │
└────────────────────────────────────────────────────────────────────┘

Normal Transaction (Read-Write):
Client → Sequencer → Paxos → Shards → Client
         (~8.5ms total latency)

Read-Only Transaction (Optimized):
Client → Shard Leader → Client
         (~1ms total latency)

Conditions:
1. Transaction declared read-only upfront
2. Read from leader replica
3. Read at latest committed sequence number
4. No sequencing needed (doesn't affect order)
```

**Read-Only Optimization**:

```
on_read_only_txn(txn):
    if txn is read-only:
        1. Route directly to shard leaders
        2. Acquire read locks (or MVCC snapshot)
        3. Execute reads
        4. Return results
        5. No sequencing, no Paxos
    else:
        // Follow normal sequencing path
```

---

### Replica Synchronization

```
┌────────────────────────────────────────────────────────────────────┐
│              ACTIVE REPLICATION ACROSS SHARDS                       │
└────────────────────────────────────────────────────────────────────┘

Shard 1 (3 replicas)         Shard 2 (3 replicas)

Replica A   Replica B   Replica C       Replica A   Replica B   Replica C
    │           │           │               │           │           │
    │ Batch (seq 100-199)   │               │ Batch (seq 100-199)   │
    ├───────────┼───────────┤               ├───────────┼───────────┤
    │           │           │               │           │           │
    │ Execute T(seq=100)    │               │ Execute T(seq=100)    │
    │           │           │               │           │           │
    │ Read X=5  │ Read X=5  │ Read X=5      │ Read Y=10 │ Read Y=10 │ Read Y=10
    │ Write X=6 │ Write X=6 │ Write X=6     │ Write Y=11│ Write Y=11│ Write Y=11
    │ Commit    │ Commit    │ Commit        │ Commit    │ Commit    │ Commit
    │           │           │               │           │           │
    └───────────┴───────────┘               └───────────┴───────────┘
         All same result                         All same result
```

**Key Properties**:

1. **Same Input**: All replicas receive same batch order (Paxos)
2. **Same Execution**: Deterministic lock acquisition and execution
3. **Same Output**: All replicas produce identical state

**State Verification** (periodic):
```
Every N transactions (e.g., 10,000):
    1. Each replica computes state checksum
    2. Leader broadcasts its checksum
    3. Followers compare with leader
    4. If mismatch: Replica enters recovery mode
```

---

## Integration with Existing Mako

### Hybrid Mode Architecture

The sequencing layer can coexist with existing Mako protocols:

```
┌────────────────────────────────────────────────────────────────────┐
│                    HYBRID ARCHITECTURE                              │
└────────────────────────────────────────────────────────────────────┘

Client submits txn with mode flag:

┌────────────────┐
│  Transaction   │
│  ─────────────│
│  mode: CALVIN  │ ──→ Distributed Sequencing Path
│  or           │
│  mode: MAKO    │ ──→ Original Speculative Path
└────────────────┘

Configuration-based routing:
- Config: "default_mode": "calvin" | "mako" | "adaptive"
- Adaptive: Use Calvin for geo-replicated, Mako for local
```

### Integration Points

1. **Client Layer** (`src/mako/lib/client.h`):
```
Add method:
    InvokeCalvinTransaction(txn)
        → Route to sequencer instead of direct shard access
```

2. **Server Layer** (`src/mako/lib/server.h`):
```
Add handlers:
    HandleSequencedBatch(batch)
    HandleDeterministicExecute(txn)
```

3. **Configuration** (`config/*.yml`):
```yaml
mode: "calvin"  # or "mako" or "hybrid"

calvin:
  batch_interval_ms: 10
  batch_max_size: 10000
  sequencer_replicas: 3
  paxos_quorum: 2

mako:
  # existing mako config
```

4. **Protocol Selection**:
```
In Coordinator::Submit():
    if config.mode == "calvin":
        return SubmitToSequencer(txn)
    else:
        return SubmitToMako(txn)
```

---

## Performance Considerations

### Throughput Analysis

**Theoretical Max Throughput**:

```
Assumptions:
- Batch window: 10ms
- Batch size limit: 10,000 txns
- Paxos latency: 1ms (negligible vs batch window)

Throughput = Batch size / Batch window
           = 10,000 txns / 10ms
           = 1,000,000 TPS

Scaling:
- Single sequencer: 1M TPS
- Partitioned sequencing (10 partitions): 10M TPS
```

**Bottlenecks**:

1. **Sequencer CPU**: Batching and Paxos processing
   - Mitigation: Partition sequencing by key range
2. **Network Bandwidth**: Forwarding batches to shards
   - Mitigation: Compression, multicast
3. **Execution Locks**: Lock contention on hot keys
   - Mitigation: Dependency analysis, early lock acquisition

### Latency Analysis

**Components**:

```
Component                  Latency      Optimization
──────────────────────────────────────────────────────
Client → Sequencer         0.5ms        Locality
Batching wait (avg)        5ms          Reduce window to 5ms
Paxos replication          1ms          Leader lease
Sequencer → Shards         0.5ms        Multicast
Execution queue wait       0-10ms       Priority queue
Lock acquisition           0-5ms        Dependency prediction
Execution                  1ms          Optimized storage
Commit                     0.5ms        Async replication
─────────────────────────────────────────────────────
TOTAL                      8.5-23ms

Target: <10ms p99
```

**Optimizations**:

1. **Reduce batch window**: 10ms → 5ms (halves wait time)
2. **Reconstitution txns**: Execute reads before sequencing
3. **Dependency prefetching**: Acquire locks early
4. **Speculative execution**: Execute before Paxos commit

### Scalability Strategies

**Horizontal Scaling**:

```
┌────────────────────────────────────────────────────────────────────┐
│              PARTITIONED SEQUENCING                                 │
└────────────────────────────────────────────────────────────────────┘

                      Hash(txn.keys) → Partition

Sequencer 1          Sequencer 2          Sequencer 3
(keys A-H)           (keys I-P)           (keys Q-Z)
    │                    │                    │
    ├→ Shard 1.A-H       ├→ Shard 2.I-P       ├→ Shard 3.Q-Z
    ├→ Shard 2.A-H       ├→ Shard 3.I-P       ├→ Shard 1.Q-Z
    └→ Shard 3.A-H       └→ Shard 1.I-P       └→ Shard 2.Q-Z

Cross-partition txns require coordination (2PC or deterministic commit)
```

**Geographic Replication**:

```
Region 1 (US-East)       Region 2 (EU)         Region 3 (Asia)
Sequencer + Shards       Sequencer + Shards    Sequencer + Shards
       │                        │                      │
       └────────── Global Paxos Replication ───────────┘
                  (WAN latency: 50-200ms)

Optimization: Use Paxos only for cross-region txns
              Local txns use regional sequencer
```

---

## Testing Strategy

### Unit Tests

1. **Batching Logic**:
   - Batch seals at timeout
   - Batch seals at size limit
   - Sequence number assignment correctness

2. **Paxos Protocol**:
   - Prepare/Accept/Commit phases
   - Leader election
   - Follower catch-up

3. **Deterministic Execution**:
   - Lock queue ordering
   - Same input → same output
   - Cross-shard coordination

### Integration Tests

1. **End-to-End Flow**:
   - Client → Sequencer → Paxos → Shards → Client
   - Verify correct result
   - Measure latency

2. **Multi-Shard Transactions**:
   - Transaction spans 2+ shards
   - All shards commit together
   - Verify atomicity

3. **Replica Consistency**:
   - 3 replicas execute same transactions
   - Verify identical state (checksums)

### Fault Tolerance Tests

1. **Sequencer Failures**:
   - Leader sequencer crashes
   - Verify leader election
   - Verify no transaction loss

2. **Shard Failures**:
   - Replica crashes during execution
   - Verify recovery from other replicas
   - Verify state consistency after recovery

3. **Network Partitions**:
   - Partition sequencer from shards
   - Partition shards from each other
   - Verify correct behavior (block or proceed with quorum)

### Performance Tests

1. **Throughput Benchmark**:
   - TPC-C with varying contention
   - Target: >100K TPS single sequencer
   - Target: >1M TPS with partitioning

2. **Latency Benchmark**:
   - Measure p50, p90, p99 latencies
   - Target: <10ms p99 for local deployment
   - Target: <200ms p99 for geo-replication

3. **Scalability Test**:
   - Vary number of shards (1, 2, 4, 8, 16)
   - Vary number of replicas (1, 3, 5)
   - Verify linear throughput scaling

### Correctness Tests

1. **Serializability**:
   - Run anomaly detection workloads (Jepsen-style)
   - Verify no lost updates, dirty reads, etc.

2. **Determinism Verification**:
   - Run same workload on 3 replicas
   - Compare final state
   - Any divergence = bug

3. **Reconfiguration Tests**:
   - Add/remove sequencer replicas
   - Add/remove shard replicas
   - Verify no data loss or inconsistency

---

## Summary of Key Decisions

### Architectural Choices

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Sequencing mechanism | Multi-Paxos batching | Proven, fault-tolerant consensus |
| Batch window | 10ms (configurable) | Balance latency vs throughput |
| Lock management | Deterministic queue | Ensures replica consistency |
| Replication model | Active replication | Simpler than state machine replication |
| Read optimization | Direct to leader | Avoid sequencing overhead for read-only |
| Partitioning | Key-range based | Compatible with existing shard architecture |

### Trade-offs

| Aspect | Gain | Cost |
|--------|------|------|
| Batching | High throughput (1M+ TPS) | Added latency (~5ms avg) |
| Paxos replication | Fault tolerance | Coordination overhead (~1ms) |
| Deterministic locks | No 2PC needed | Potential lock contention |
| Active replication | Simple, consistent | All replicas do work (3x CPU) |
| Geo-replication | Disaster recovery | WAN latency (50-200ms) |

---

## Next Steps

1. **Review with team**: Discuss architectural decisions
2. **Prototype Phase 1**: Build single-sequencer version (2 weeks)
3. **Evaluate performance**: Benchmark against existing Mako
4. **Iterate on design**: Adjust based on prototype learnings
5. **Proceed with Phases 2-6**: Full implementation (10 weeks)

---

## References

- **Calvin**: "Calvin: Fast Distributed Transactions for Partitioned Database Systems" (SIGMOD 2012)
- **Multi-Paxos**: "Paxos Made Simple" (Lamport, 2001)
- **Deterministic Databases**: "An Evaluation of Distributed Concurrency Control" (Harding et al., VLDB 2017)
- **Mako Architecture**: See `CLAUDE.md` in this repository

---

*This plan is a living document and will be updated as implementation progresses.*
