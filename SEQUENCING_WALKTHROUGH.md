# Distributed Sequencing: Step-by-Step Walkthrough

This document walks through concrete examples of how distributed sequencing works in various scenarios.

---

## Example 1: Simple Single-Shard Transaction

**Scenario**: Client wants to increment a counter stored on Shard 1

**Transaction**: `UPDATE counters SET value = value + 1 WHERE id = 'total_orders'`

### Step-by-Step Execution

#### Step 1: Client Submission (T=0ms)

```
Client Application
    │
    │ transaction = {
    │   sql: "UPDATE counters SET value = value + 1 WHERE id = 'total_orders'",
    │   read_set: ['counter:total_orders'],
    │   write_set: ['counter:total_orders']
    │ }
    │
    ▼
Send to Sequencer
```

**What happens**:
- Client constructs transaction request
- Identifies which keys will be read/written
- Sends RPC to sequencer: `SubmitTransaction(txn)`

**Sequencer state**:
- `current_batch.txns = [..., txn_new]`
- `current_batch.size = 4,237` (not full yet)
- `batch_timer.remaining = 3.2ms` (not expired yet)

---

#### Step 2: Batching Wait (T=0ms - T=10ms)

```
Sequencer
    │
    │ current_batch = {
    │   txns: [txn_1, txn_2, ..., txn_4237],
    │   start_time: T=0ms
    │ }
    │
    │ Wait for batch seal condition:
    │   - Size >= 10,000 txns? NO (4,237 < 10,000)
    │   - Time >= 10ms? NO (3.2ms < 10ms)
    │
    │ ... more transactions arrive ...
    │
    │ At T=10ms:
    │   - Time >= 10ms? YES
    │
    ▼
Seal batch
```

**What happens**:
- Transaction sits in sequencer's current batch
- Other clients submit more transactions
- At T=10ms, timer expires
- Batch contains 8,432 transactions (including ours)

**Batch sealed**:
```
sealed_batch = {
  epoch_id: 1523,
  txns: [txn_1, txn_2, ..., txn_8432],
  base_sequence_number: 152300000,
  timestamp: T=10ms
}
```

---

#### Step 3: Sequence Number Assignment (T=10ms)

```
Sequencer
    │
    │ sealed_batch.base_sequence_number = next_sequence_number
    │ next_sequence_number = 152300000
    │
    │ For each txn in sealed_batch.txns:
    │   txn[i].sequence_number = base_sequence_number + i
    │
    │ Our transaction gets:
    │   txn.sequence_number = 152300000 + 4236 = 152304236
    │
    │ next_sequence_number += batch.size
    │ next_sequence_number = 152300000 + 8432 = 152308432
    │
    ▼
Batch ready for Paxos
```

**What happens**:
- Sequencer assigns globally unique sequence numbers
- Each transaction in batch gets consecutive number
- Sequence numbers are never reused
- Counter is updated atomically

---

#### Step 4: Paxos Replication (T=10ms - T=11ms)

```
Sequencer 1 (Leader)              Sequencer 2              Sequencer 3
     │                                  │                        │
     │ Propose:                         │                        │
     │ {slot: 1523,                     │                        │
     │  batch: sealed_batch}            │                        │
     ├─────────────────────────────────→│                        │
     ├──────────────────────────────────┼───────────────────────→│
     │                                  │                        │
     │                                  │ Log batch              │ Log batch
     │                                  │                        │
     │ Accept(slot: 1523)               │                        │
     │←─────────────────────────────────┤                        │
     │←─────────────────────────────────┼────────────────────────┤
     │                                  │                        │
     │ Quorum reached (2/3)             │                        │
     │                                  │                        │
     │ Commit(slot: 1523)               │                        │
     ├─────────────────────────────────→│                        │
     ├──────────────────────────────────┼───────────────────────→│
     │                                  │                        │
     ▼                                  ▼                        ▼
Forward batch to shards          Forward batch           Forward batch
```

**What happens**:
- Leader sequencer proposes batch to followers
- Followers log the batch to persistent storage
- Followers send ACK back to leader
- Once quorum (2 out of 3) ACKs received, batch is committed
- All sequencers forward batch to execution shards

**Timing**: 1 RTT = ~1ms (local network)

---

#### Step 5: Shard Reception (T=11ms)

```
Shard 1 (owns 'counter:total_orders')
     │
     │ Receive batch from Sequencer
     │
     ▼
ReceiveSequencedBatch(batch) {
     │
     │ Verify batch integrity:
     │   - Check epoch_id = previous + 1? ✓
     │   - Check sequence numbers continuous? ✓
     │   - Check checksum? ✓
     │
     │ Extract relevant transactions:
     │   - Filter txns that access keys on this shard
     │
     │ Our transaction:
     │   seq=152304236
     │   keys=['counter:total_orders'] → This shard!
     │
     │ Add to execution queue:
     │   pending_queue.insert(txn, priority=152304236)
     │
     ▼
}
```

**What happens**:
- Shard receives the entire batch
- Filters for transactions accessing its keys
- Adds those transactions to priority queue (ordered by sequence number)
- Queue now contains: [..., txn_152304235, **txn_152304236**, txn_152304237, ...]

---

#### Step 6: Waiting for Turn (T=11ms - T=12ms)

```
Shard 1 Execution Queue
     │
     │ Current state:
     │   last_executed_sequence = 152304230
     │   next_expected_sequence = 152304231
     │
     │ Queue (priority order):
     │   [152304231: LOCKING,
     │    152304232: QUEUED,
     │    152304233: QUEUED,
     │    152304234: QUEUED,
     │    152304235: QUEUED,
     │    152304236: QUEUED,  ← Our transaction (waiting)
     │    152304237: QUEUED,
     │    ...]
     │
     │ Txn 152304231 finishes execution
     │ Txn 152304232 starts execution
     │ ...
     │ Txn 152304235 finishes execution
     │
     │ Now:
     │   next_expected_sequence = 152304236
     │
     ▼
Start our transaction
```

**What happens**:
- Shard processes transactions in strict sequence order
- Each transaction must wait for previous one to finish
- This ensures deterministic execution
- When our turn comes (seq=152304236), we proceed

---

#### Step 7: Lock Acquisition (T=12ms)

```
Shard 1 Deterministic Scheduler
     │
     │ execute_transaction(txn_152304236) {
     │
     │   // Sort keys for deterministic lock order
     │   keys = sort(txn.read_set ∪ txn.write_set)
     │   keys = ['counter:total_orders']
     │
     │   // Try to acquire locks
     │   for key in keys:
     │     acquire_lock(key, txn.sequence_number)
     │
     │   acquire_lock('counter:total_orders', 152304236) {
     │
     │     lock_queue = lock_queues['counter:total_orders']
     │
     │     if lock_queue.is_empty():
     │       // Lock is free!
     │       lock_queue.grant(152304236)
     │       return SUCCESS
     │     else:
     │       // Someone holds the lock
     │       lock_queue.enqueue(152304236)
     │       wait_for_lock()
     │   }
     │
     │   // In our case, lock was free
     │   // All locks acquired!
     │
     ▼
Proceed to execution
```

**What happens**:
- Scheduler tries to acquire lock on 'counter:total_orders'
- If lock is free: Grant immediately
- If lock is held: Add to queue and wait
- In this case, lock was free (or previous holder just released it)
- Transaction now holds exclusive lock

**Lock state**:
```
lock_queues['counter:total_orders'] = {
  holder: txn_152304236,
  queue: []
}
```

---

#### Step 8: Execute Transaction Logic (T=12ms)

```
Shard 1 Storage Engine
     │
     │ execute_pieces(txn_152304236) {
     │
     │   // Read current value
     │   old_value = storage.read('counter:total_orders')
     │   old_value = 42,857
     │
     │   // Compute new value
     │   new_value = old_value + 1
     │   new_value = 42,858
     │
     │   // Write to buffer (not committed yet)
     │   txn.write_buffer['counter:total_orders'] = 42,858
     │
     │   // Check for errors (e.g., constraint violations)
     │   if no_errors:
     │     return COMMIT_VOTE
     │   else:
     │     return ABORT_VOTE
     │
     │   // In our case: SUCCESS
     │   return COMMIT_VOTE
     │
     ▼
}
```

**What happens**:
- Transaction executes its SQL logic
- Reads current counter value (42,857)
- Increments it (42,858)
- Writes to temporary buffer (not storage yet)
- Returns COMMIT vote (no errors)

---

#### Step 9: Commit Decision (T=12ms)

```
Shard 1 Commit Protocol
     │
     │ // For single-shard transactions, simple commit
     │
     │ commit(txn_152304236) {
     │
     │   // Apply buffered writes to storage
     │   for (key, value) in txn.write_buffer:
     │     storage.write(key, value)
     │
     │   storage.write('counter:total_orders', 42,858)
     │
     │   // Mark transaction as committed
     │   txn.status = COMMITTED
     │
     │   // Update committed sequence number
     │   last_committed_sequence = 152304236
     │
     │   // Release locks
     │   for key in txn.locks:
     │     release_lock(key)
     │
     │   release_lock('counter:total_orders') {
     │     lock_queue = lock_queues['counter:total_orders']
     │     lock_queue.release()
     │
     │     if !lock_queue.is_empty():
     │       next_txn = lock_queue.dequeue()
     │       lock_queue.grant(next_txn)
     │       wake_up(next_txn)
     │   }
     │
     │   // Advance to next transaction
     │   next_expected_sequence = 152304237
     │
     ▼
}
```

**What happens**:
- Transaction writes committed to persistent storage (Masstree/RocksDB)
- Counter value now permanently updated: 42,857 → 42,858
- Locks released
- Next transaction in queue (152304237) granted the lock
- Shard advances to next sequence number

---

#### Step 10: Response to Client (T=13ms)

```
Shard 1                          Sequencer                     Client
     │                                │                            │
     │ Transaction committed          │                            │
     │                                │                            │
     │ Send result                    │                            │
     ├───────────────────────────────→│                            │
     │ {txn_id, status: COMMITTED}    │                            │
     │                                │                            │
     │                                │ Notify client              │
     │                                ├───────────────────────────→│
     │                                │ {result: SUCCESS}          │
     │                                │                            │
```

**What happens**:
- Shard sends commit acknowledgment to sequencer (or directly to client)
- Client receives SUCCESS response
- Transaction complete!

**Total Latency Breakdown**:
- Client → Sequencer: 0.5ms
- Batching wait (average): 5ms
- Paxos replication: 1ms
- Sequencer → Shard: 0.5ms
- Execution queue wait: 1ms
- Lock acquisition: 0ms (immediate)
- Execution: 0.5ms
- Commit: 0.5ms
- Response: 0.5ms
- **Total: ~10ms**

---

## Example 2: Cross-Shard Transaction

**Scenario**: Transfer $100 from Account A (Shard 1) to Account B (Shard 2)

**Transaction**:
```sql
BEGIN;
UPDATE accounts SET balance = balance - 100 WHERE id = 'A';
UPDATE accounts SET balance = balance + 100 WHERE id = 'B';
COMMIT;
```

### Step-by-Step Execution

#### Steps 1-5: Same as Example 1

- Client submits transaction
- Batching at sequencer
- Sequence number assignment: seq = 200000000
- Paxos replication
- Batch forwarded to **both** Shard 1 and Shard 2

---

#### Step 6: Both Shards Receive Transaction

```
Shard 1 (Account A)                           Shard 2 (Account B)
     │                                              │
     │ ReceiveSequencedBatch(batch)                 │ ReceiveSequencedBatch(batch)
     │                                              │
     │ Extract transaction 200000000:               │ Extract transaction 200000000:
     │   pieces: [write account A]                  │   pieces: [write account B]
     │                                              │
     │ Add to queue:                                │ Add to queue:
     │   pending[200000000] = txn                   │   pending[200000000] = txn
     │                                              │
     ▼                                              ▼
```

**Key insight**: Both shards receive the **same** batch with the **same** sequence number

---

#### Step 7: Parallel Execution on Both Shards

```
Shard 1                                       Shard 2
     │                                              │
     │ Wait for seq=200000000                       │ Wait for seq=200000000
     │                                              │
     │ Acquire lock on 'account:A'                  │ Acquire lock on 'account:B'
     │                                              │
     │ Execute:                                     │ Execute:
     │   old_balance = read('account:A')            │   old_balance = read('account:B')
     │   old_balance = $500                         │   old_balance = $200
     │                                              │
     │   new_balance = $500 - $100 = $400           │   new_balance = $200 + $100 = $300
     │                                              │
     │   write_buffer['account:A'] = $400           │   write_buffer['account:B'] = $300
     │                                              │
     │   vote = COMMIT (balance sufficient)         │   vote = COMMIT (no errors)
     │                                              │
     ▼                                              ▼
Local execution complete                    Local execution complete
```

**Both shards execute in parallel** - no waiting for each other during execution!

---

#### Step 8: Cross-Shard Coordination

```
Shard 1 (Participant)                         Shard 2 (Coordinator)
     │                                              │
     │ Local execution: COMMIT vote                 │ Local execution: COMMIT vote
     │                                              │
     │ Send PREPARE_OK to coordinator               │
     ├─────────────────────────────────────────────→│
     │                                              │
     │                                              │ Collect votes:
     │                                              │   Shard 1: COMMIT ✓
     │                                              │   Shard 2: COMMIT ✓
     │                                              │
     │                                              │ All votes = COMMIT?
     │                                              │   YES → Decision: COMMIT
     │                                              │   NO  → Decision: ABORT
     │                                              │
     │                                              │ Decision: COMMIT
     │                                              │
     │ Receive COMMIT decision                      │
     │←─────────────────────────────────────────────┤
     │                                              │
     ▼                                              ▼
Apply writes                                  Apply writes
```

**Coordination protocol**:
1. One shard designated as "coordinator" (e.g., first shard in sorted order)
2. Other shards send their vote to coordinator
3. Coordinator collects all votes
4. Makes commit/abort decision
5. Broadcasts decision to all participants

---

#### Step 9: Deterministic Commit

```
Shard 1                                       Shard 2
     │                                              │
     │ Apply write:                                 │ Apply write:
     │   storage.write('account:A', $400)           │   storage.write('account:B', $300)
     │                                              │
     │ Release lock on 'account:A'                  │ Release lock on 'account:B'
     │                                              │
     │ Mark txn 200000000 as COMMITTED              │ Mark txn 200000000 as COMMITTED
     │                                              │
     │ Advance: next_sequence = 200000001           │ Advance: next_sequence = 200000001
     │                                              │
     ▼                                              ▼
DONE                                          DONE
```

**Final state**:
- Account A: $500 → $400 ✓
- Account B: $200 → $300 ✓
- **Atomic transfer successful!**

---

### Why No 2PC Needed?

**Traditional 2PC problem**:
```
Scenario: Shard 1 commits, then Shard 2 crashes before commit
Result: Account A debited, Account B not credited (money lost!)
```

**Calvin's deterministic execution**:
```
Scenario: Shard 2 crashes before commit
What happens:
  1. Shard 2 reboots
  2. Replays sequence log from last checkpoint
  3. Sees transaction 200000000 in log
  4. Re-executes deterministically:
     - Same input (seq=200000000, piece: write B +$100)
     - Same execution (read $200, write $300)
     - Same decision (COMMIT)
  5. Applies write: account B = $300
  6. Catches up to current sequence

Result: Eventually consistent - both shards commit!
```

**Key property**: Deterministic execution guarantees all replicas reach same decision

---

## Example 3: Replica Consistency

**Scenario**: Shard 1 has 3 replicas (A, B, C) executing the same transaction

**Transaction**: `UPDATE inventory SET quantity = quantity - 1 WHERE product = 'iPhone'`

**Sequence number**: 300000000

### All Replicas Execute Identically

```
Replica A               Replica B               Replica C
    │                       │                       │
    │ Receive batch         │ Receive batch         │ Receive batch
    │ (seq 300000000)       │ (seq 300000000)       │ (seq 300000000)
    │                       │                       │
    ▼                       ▼                       ▼
Wait for seq=300000000  Wait for seq=300000000  Wait for seq=300000000
    │                       │                       │
    ▼                       ▼                       ▼
Acquire lock            Acquire lock            Acquire lock
on 'inventory:iPhone'   on 'inventory:iPhone'   on 'inventory:iPhone'
    │                       │                       │
    ▼                       ▼                       ▼
Execute:                Execute:                Execute:
  read qty = 157          read qty = 157          read qty = 157
  write qty = 156         write qty = 156         write qty = 156
    │                       │                       │
    ▼                       ▼                       ▼
Commit                  Commit                  Commit
    │                       │                       │
    ▼                       ▼                       ▼
State: qty = 156        State: qty = 156        State: qty = 156
```

**Why identical execution?**

1. **Same Input**:
   - All receive same batch (Paxos replication)
   - Same sequence number (300000000)
   - Same transaction content

2. **Same Order**:
   - All execute seq=300000000 at the same point in sequence
   - Same lock acquisition order (deterministic)

3. **Same State**:
   - All started with qty=157 (previous transactions were also identical)
   - All read qty=157
   - All write qty=156

4. **Same Output**:
   - All commit
   - All end with qty=156

**Verification**:
```
Every 10,000 transactions:
  Replica A: checksum(state) = 0x8A3F2B1D
  Replica B: checksum(state) = 0x8A3F2B1D
  Replica C: checksum(state) = 0x8A3F2B1D

  All match! ✓
```

---

## Example 4: Failure Recovery

**Scenario**: Replica B crashes during execution, then recovers

### Timeline

#### T=0: Normal Operation

```
Replica A               Replica B               Replica C
    │                       │                       │
    │ Execute seq=1000      │ Execute seq=1000      │ Execute seq=1000
    │ Execute seq=1001      │ Execute seq=1001      │ Execute seq=1001
    │ Execute seq=1002      │ Execute seq=1002      │ Execute seq=1002
```

State at seq=1002:
- All replicas: `{accounts: {A: $500, B: $200}, ...}`

---

#### T=1: Replica B Crashes

```
Replica A               Replica B               Replica C
    │                       │                       │
    │ Execute seq=1003      │ ✗ CRASH               │ Execute seq=1003
    │ Execute seq=1004      │                       │ Execute seq=1004
    │ Execute seq=1005      │                       │ Execute seq=1005
```

State:
- Replica A: seq=1005
- Replica B: **DOWN** (last: seq=1002)
- Replica C: seq=1005

**Client impact**: None! Requests routed to A and C

---

#### T=2: Replica B Reboots

```
Replica B
    │
    │ REBOOT
    │
    │ 1. Load last checkpoint
    │    - Checkpoint at seq=1000
    │    - State: {accounts: {A: $500, B: $200}}
    │
    │ 2. Scan WAL (Write-Ahead Log)
    │    - Find committed transactions: 1001, 1002
    │    - State after WAL: {accounts: {A: $500, B: $200}}
    │                        (transactions 1001-1002 applied)
    │
    │ 3. Check with peers
    │    - "What's the current sequence?"
    │    - Replica A: "I'm at seq=1005"
    │    - Replica C: "I'm at seq=1005"
    │
    │ 4. Request missing batches
    │    - "Send me batches for seq 1003-1005"
    │
    ▼
```

---

#### T=3: Catch-Up

```
Replica A                                   Replica B
    │                                           │
    │ "Here's batch for seq 1003-1005"          │
    ├──────────────────────────────────────────→│
    │                                           │
    │                                           │ Replay seq=1003
    │                                           │   (same as A executed)
    │                                           │
    │                                           │ Replay seq=1004
    │                                           │   (same as A executed)
    │                                           │
    │                                           │ Replay seq=1005
    │                                           │   (same as A executed)
    │                                           │
    │                                           │ State: {accounts: {A: $450, B: $250}}
    │                                           │   (matches A and C!)
    │                                           │
    │ Meanwhile: Execute seq=1006               │
    ├──────────────────────────────────────────→│ Also execute seq=1006
    │                                           │   (now caught up!)
    │                                           │
    ▼                                           ▼
```

**Recovery complete!** Replica B is now in sync.

**Key properties**:
- Deterministic replay: Same batches → Same state
- No manual intervention needed
- Automatic catch-up
- Eventually consistent (guaranteed)

---

## Example 5: Read-Only Transaction Optimization

**Scenario**: Client wants to read account balances (no writes)

**Traditional flow** (with sequencing):
- Client → Sequencer → Batch → Paxos → Shard → Execute → Client
- Latency: ~10ms

**Optimized flow** (bypass sequencing):
- Client → Shard Leader → Execute → Client
- Latency: ~1ms

### Step-by-Step Optimized Read

#### Step 1: Client Declares Read-Only

```
Client
    │
    │ transaction = {
    │   sql: "SELECT balance FROM accounts WHERE id IN ('A', 'B')",
    │   read_set: ['account:A', 'account:B'],
    │   write_set: [],  ← Empty!
    │   read_only: true  ← Optimization flag
    │ }
    │
    │ Route directly to Shard Leader (skip sequencer)
    │
    ▼
Shard 1 Leader
```

---

#### Step 2: Shard Takes Snapshot

```
Shard 1 Leader
    │
    │ on_read_only_transaction(txn) {
    │
    │   // Take snapshot at latest committed sequence
    │   snapshot_seq = last_committed_sequence
    │   snapshot_seq = 500000000
    │
    │   // Create read view at this snapshot
    │   read_view = create_snapshot(500000000)
    │
    │   // Read data at snapshot
    │   balance_A = storage.read('account:A', read_view)
    │   balance_A = $450
    │
    │   balance_B = storage.read('account:B', read_view)
    │   balance_B = $250
    │
    │   // Return results immediately
    │   return {A: $450, B: $250}
    │
    ▼
}
```

**No locks needed!** Read-only, so no conflicts.

---

#### Step 3: Return to Client

```
Shard 1 Leader                          Client
     │                                      │
     │ {A: $450, B: $250}                   │
     ├─────────────────────────────────────→│
     │                                      │
     │ Latency: ~1ms                        │
     │                                      │
```

**Latency Breakdown**:
- Client → Shard: 0.5ms
- Snapshot read: 0.1ms
- Shard → Client: 0.5ms
- **Total: ~1ms** (vs ~10ms with sequencing)

**Trade-offs**:
- ✅ Much faster (10x improvement)
- ✅ No sequencing overhead
- ⚠️ May read slightly stale data (snapshot may be a few milliseconds behind)
- ⚠️ Only works for read-only transactions

---

## Summary of How Distributed Sequencing Works

### Core Principles

1. **Global Ordering**:
   - Sequencer assigns unique, monotonic sequence numbers
   - All shards receive transactions in the same order

2. **Batching for Throughput**:
   - Transactions grouped into batches (10ms windows)
   - Amortizes coordination overhead
   - Enables high throughput (1M+ TPS)

3. **Paxos for Consensus**:
   - Sequencer replicas agree on batch order using Paxos
   - Fault tolerant: Survives sequencer failures
   - Consistent: All replicas see same order

4. **Deterministic Execution**:
   - Shards execute transactions in sequence number order
   - Lock acquisition is deterministic (sorted keys)
   - Same input → Same output (guaranteed)

5. **Active Replication**:
   - All replicas execute all transactions
   - No primary-backup: All replicas are equal
   - Consistency via determinism (not coordination)

6. **No 2PC Needed**:
   - Cross-shard transactions commit/abort deterministically
   - Failures recovered by replaying sequence log
   - Simpler, faster than traditional 2PC

### Performance Characteristics

| Aspect | Value |
|--------|-------|
| Throughput | 1M+ TPS (single sequencer) |
| Latency | ~10ms (with batching) |
| Read-only latency | ~1ms (optimized) |
| Failure recovery | Automatic, seconds |
| Scalability | Linear with shards |

### Key Advantages

✅ **Strong Consistency**: Serializable isolation without conflicts
✅ **High Throughput**: Batching amortizes overhead
✅ **Fault Tolerance**: Paxos replication survives failures
✅ **Simplicity**: No distributed deadlock detection, no 2PC
✅ **Predictable Performance**: Deterministic execution → Consistent latency

### Key Limitations

⚠️ **Batching Latency**: ~5-10ms added latency due to batching
⚠️ **Sequential Bottleneck**: Single sequencer limits global throughput
⚠️ **Geographic Latency**: WAN Paxos adds 50-200ms for geo-replication
⚠️ **Write Amplification**: All replicas execute all writes

---

## Next Steps

- Review implementation plan: `DISTRIBUTED_SEQUENCING_PLAN.md`
- Study detailed diagrams: `SEQUENCING_DIAGRAMS.md`
- Begin Phase 1 implementation (basic sequencing)
- Benchmark performance vs existing Mako protocols

---

*This walkthrough demonstrates how distributed sequencing provides strong consistency, fault tolerance, and high throughput through deterministic execution.*
