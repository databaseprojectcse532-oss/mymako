# Distributed Sequencing - Detailed Diagrams

This document provides detailed visual diagrams for understanding distributed sequencing in Mako.

---

## 1. Transaction State Machine

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    TRANSACTION STATE TRANSITIONS                          │
└──────────────────────────────────────────────────────────────────────────┘

                            ┌──────────────┐
                            │  SUBMITTED   │
                            │  (at client) │
                            └──────┬───────┘
                                   │
                        Client submits to Sequencer
                                   │
                                   ▼
                            ┌──────────────┐
                            │   BATCHED    │
                            │(in sequencer)│
                            └──────┬───────┘
                                   │
                        Batch sealed + Paxos
                                   │
                                   ▼
                            ┌──────────────┐
                            │  SEQUENCED   │
                            │(assigned seq#)│
                            └──────┬───────┘
                                   │
                        Forwarded to shards
                                   │
                                   ▼
                            ┌──────────────┐
                            │   QUEUED     │
                            │(waiting turn)│
                            └──────┬───────┘
                                   │
                        Sequence # reached
                                   │
                                   ▼
                            ┌──────────────┐
                            │   LOCKING    │
                            │(acquiring    │
                            │  locks)      │
                            └──────┬───────┘
                                   │
                        All locks acquired
                                   │
                                   ▼
                            ┌──────────────┐
                            │  EXECUTING   │
                            │(running SQL) │
                            └──────┬───────┘
                                   │
                        Execution completes
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
             ┌──────────┐   ┌──────────┐  ┌──────────┐
             │COMMITTING│   │ ABORTING │  │  FAILED  │
             │(cross-   │   │(conflict │  │  (error) │
             │ shard)   │   │detected) │  │          │
             └────┬─────┘   └────┬─────┘  └────┬─────┘
                  │              │             │
                  ▼              ▼             ▼
             ┌──────────┐   ┌──────────┐  ┌──────────┐
             │COMMITTED │   │ ABORTED  │  │  ABORTED │
             │(success) │   │          │  │          │
             └────┬─────┘   └────┬─────┘  └────┬─────┘
                  │              │             │
                  └──────────────┴─────────────┘
                                 │
                          Release locks
                                 │
                                 ▼
                            ┌──────────────┐
                            │  COMPLETED   │
                            └──────────────┘
```

---

## 2. Sequencer Internal Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│                      SEQUENCER INTERNAL COMPONENTS                        │
└──────────────────────────────────────────────────────────────────────────┘

                             Client Requests
                                    │
                                    ▼
                    ┌────────────────────────────────┐
                    │     Request Handler Pool       │
                    │  (multi-threaded receivers)    │
                    └────────────────┬───────────────┘
                                     │
                                     ▼
                    ┌────────────────────────────────┐
                    │      Transaction Queue         │
                    │  (lock-free, wait-free queue)  │
                    └────────────────┬───────────────┘
                                     │
                                     ▼
        ┌────────────────────────────────────────────────────┐
        │              Batch Builder Thread                  │
        │  ┌──────────────────────────────────────────────┐  │
        │  │  Current Batch                               │  │
        │  │  - txns: vector<Transaction>                 │  │
        │  │  - size: atomic<int>                         │  │
        │  │  - start_time: timestamp                     │  │
        │  └──────────────────────────────────────────────┘  │
        │                                                     │
        │  Conditions to seal batch:                         │
        │  1. Size >= 10,000 txns                            │
        │  2. Time >= 10ms since start                       │
        │  3. Manual trigger (for testing)                   │
        └────────────────────┬───────────────────────────────┘
                             │
                             ▼ Batch sealed
        ┌────────────────────────────────────────────────────┐
        │           Sequence Number Allocator                │
        │  - next_seq: atomic<uint64_t>                      │
        │  - Assigns: [next_seq, next_seq + batch.size)      │
        │  - Atomic increment: next_seq += batch.size        │
        └────────────────────┬───────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────────────────┐
        │              Paxos Coordinator                     │
        │  ┌──────────────────────────────────────────────┐  │
        │  │  Propose Phase                               │  │
        │  │  - Create PaxosProposal(slot, batch)         │  │
        │  │  - Broadcast to follower sequencers          │  │
        │  │  - Wait for quorum ACKs                      │  │
        │  └──────────────────────────────────────────────┘  │
        │  ┌──────────────────────────────────────────────┐  │
        │  │  Commit Phase                                │  │
        │  │  - Broadcast COMMIT(slot, batch)             │  │
        │  │  - Log to local Paxos log                    │  │
        │  └──────────────────────────────────────────────┘  │
        └────────────────────┬───────────────────────────────┘
                             │
                             ▼ Batch committed
        ┌────────────────────────────────────────────────────┐
        │            Batch Distribution Thread               │
        │  - For each shard:                                 │
        │    - RPC: SendSequencedBatch(batch)                │
        │  - Optimization: Multicast if supported            │
        └────────────────────┬───────────────────────────────┘
                             │
                             ▼
                    To Execution Shards
                    (all shards receive
                     same batch order)

┌──────────────────────────────────────────────────────────────────────────┐
│                       MONITORING & METRICS                                │
└──────────────────────────────────────────────────────────────────────────┘

    ┌──────────────────────────────────────────────────┐
    │  Metrics Collector                               │
    │  - Batches per second                            │
    │  - Average batch size                            │
    │  - Paxos latency (p50, p99)                      │
    │  - Sequence number lag (leader vs followers)     │
    │  - Queue depth (pending transactions)            │
    └──────────────────────────────────────────────────┘
```

---

## 3. Shard Execution Pipeline

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    SHARD EXECUTION PIPELINE                               │
└──────────────────────────────────────────────────────────────────────────┘

Batch arrives from Sequencer
         │
         ▼
┌─────────────────────────────────────────┐
│  Batch Reception Handler                │
│  - Verify batch integrity (checksum)    │
│  - Check epoch continuity               │
│  - Detect gaps in sequence              │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────┐
│  Transaction Decomposition              │
│  - Extract txns for THIS shard          │
│  - Partition by sequence number         │
│  - Build execution queue                │
└────────────────┬────────────────────────┘
                 │
                 ▼
┌─────────────────────────────────────────────────────────────────────┐
│             Deterministic Execution Queue                           │
│                                                                     │
│  Priority Queue (ordered by sequence number):                      │
│                                                                     │
│   Seq 1000  [T1: read(A), write(B)]  ──→  READY                    │
│   Seq 1001  [T2: read(B), write(C)]  ──→  WAITING (lock on B)      │
│   Seq 1002  [T3: read(A), write(A)]  ──→  WAITING (turn)           │
│   Seq 1003  [T4: read(D), write(E)]  ──→  READY (no conflict)      │
│   ...                                                               │
│                                                                     │
└─────────────────────┬───────────────────────────────────────────────┘
                      │
         ┌────────────┼────────────┐
         ▼            ▼            ▼
  ┌──────────┐ ┌──────────┐ ┌──────────┐
  │ Worker 1 │ │ Worker 2 │ │ Worker 3 │  ... (thread pool)
  └────┬─────┘ └────┬─────┘ └────┬─────┘
       │            │            │
       └────────────┼────────────┘
                    │
                    ▼
        ┌───────────────────────────────┐
        │   Lock Manager                │
        │                               │
        │   Key "A": [T1: granted]      │
        │   Key "B": [T1: granted,      │
        │             T2: waiting]      │
        │   Key "C": [free]             │
        │   ...                         │
        └───────────┬───────────────────┘
                    │
                    ▼ Locks acquired
        ┌───────────────────────────────┐
        │   Storage Engine              │
        │   (Masstree / RocksDB)        │
        │                               │
        │   Execute reads:              │
        │   - MVCC snapshot read        │
        │   - Return versioned data     │
        │                               │
        │   Execute writes:             │
        │   - Buffer in write set       │
        │   - Don't commit yet          │
        └───────────┬───────────────────┘
                    │
                    ▼ Local execution done
        ┌───────────────────────────────┐
        │   Cross-Shard Coordinator     │
        │   (for multi-shard txns)      │
        │                               │
        │   Send status to other shards │
        │   Collect votes: COMMIT/ABORT │
        │   Make final decision         │
        └───────────┬───────────────────┘
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
    ┌─────────┐          ┌─────────┐
    │ COMMIT  │          │ ABORT   │
    └────┬────┘          └────┬────┘
         │                    │
         ▼                    ▼
┌──────────────────┐  ┌──────────────────┐
│ Apply writes to  │  │ Discard writes   │
│ storage engine   │  │                  │
└────┬─────────────┘  └────┬─────────────┘
     │                     │
     └──────────┬──────────┘
                ▼
       ┌────────────────────┐
       │ Release locks      │
       │ Grant next in queue│
       └────────┬───────────┘
                │
                ▼
       ┌────────────────────┐
       │ Advance sequence   │
       │ next_expected_seq++│
       └────────┬───────────┘
                │
                ▼
       Process next transaction
```

---

## 4. Lock Queue Visualization

```
┌──────────────────────────────────────────────────────────────────────────┐
│                   DETERMINISTIC LOCK QUEUES                               │
└──────────────────────────────────────────────────────────────────────────┘

Time flows left to right ──────────────────────────────────────────────→

Lock on Key "account-123":

  Seq 1000        Seq 1005        Seq 1010        Seq 1015
  T1: +$100       T2: +$200       T3: -$50        T4: +$75
  ┌─────────┐     ┌─────────┐     ┌─────────┐     ┌─────────┐
  │EXECUTING│ ──→ │ WAITING │ ──→ │ WAITING │ ──→ │ WAITING │
  │(granted)│     │         │     │         │     │         │
  └────┬────┘     └─────────┘     └─────────┘     └─────────┘
       │
       │ T1 finishes execution
       ▼
  ┌─────────┐
  │ COMMIT  │
  └────┬────┘
       │
       │ Release lock, grant to next
       ▼

  Seq 1000        Seq 1005        Seq 1010        Seq 1015
  T1: DONE        T2: +$200       T3: -$50        T4: +$75
                  ┌─────────┐     ┌─────────┐     ┌─────────┐
                  │EXECUTING│ ──→ │ WAITING │ ──→ │ WAITING │
                  │(granted)│     │         │     │         │
                  └────┬────┘     └─────────┘     └─────────┘
                       │
                       │ T2 finishes
                       ▼
                  ┌─────────┐
                  │ COMMIT  │
                  └────┬────┘
                       │
                       │ Release lock, grant to next
                       ▼

  Seq 1000        Seq 1005        Seq 1010        Seq 1015
  T1: DONE        T2: DONE        T3: -$50        T4: +$75
                                  ┌─────────┐     ┌─────────┐
                                  │EXECUTING│ ──→ │ WAITING │
                                  │(granted)│     │         │
                                  └─────────┘     └─────────┘

┌──────────────────────────────────────────────────────────────────────────┐
│                   MULTI-KEY LOCKING EXAMPLE                               │
└──────────────────────────────────────────────────────────────────────────┘

Transaction T (seq=2000): Transfer $50 from A to B
  - Needs locks on BOTH "account-A" AND "account-B"
  - Must acquire in sorted key order for deadlock prevention

Step 1: Sort keys
  Keys: ["account-A", "account-B"]  (alphabetical order)

Step 2: Acquire lock on "account-A"
  Lock queue for "account-A":
    [T_prev(1999): granted] → [T(2000): waiting]

Step 3: Wait for "account-A" to be granted
  Lock queue for "account-A":
    [T(2000): granted]

Step 4: Acquire lock on "account-B"
  Lock queue for "account-B":
    [T(2000): granted]

Step 5: Execute transaction
  Read balance_A = $100
  Read balance_B = $50
  Write balance_A = $50
  Write balance_B = $100

Step 6: Commit & release locks
  Release "account-A" and "account-B" in reverse order

KEY INSIGHT: All replicas acquire locks in the SAME ORDER
             (sequence number + sorted keys) → No deadlocks!
```

---

## 5. Cross-Shard Transaction Protocol

```
┌──────────────────────────────────────────────────────────────────────────┐
│              CROSS-SHARD TRANSACTION EXECUTION                            │
└──────────────────────────────────────────────────────────────────────────┘

Example: Transfer $100 from Account A (Shard 1) to Account B (Shard 2)
         Transaction T with sequence number 5000

┌─────────────────┐                              ┌─────────────────┐
│    Shard 1      │                              │    Shard 2      │
│  (Account A)    │                              │  (Account B)    │
└─────────────────┘                              └─────────────────┘

Both shards receive SAME batch from sequencer:
    Batch[..., T(seq=5000, pieces=[read A, write A, read B, write B]), ...]

         │                                                │
         │ Add T to execution queue at seq=5000          │
         ▼                                                ▼
    ┌─────────┐                                      ┌─────────┐
    │ QUEUED  │                                      │ QUEUED  │
    └────┬────┘                                      └────┬────┘
         │                                                │
         │ Wait for seq=4999 to finish                    │
         │                                                │
         ▼                                                ▼
    ┌─────────┐                                      ┌─────────┐
    │ LOCKING │                                      │ LOCKING │
    │ (lock A)│                                      │ (lock B)│
    └────┬────┘                                      └────┬────┘
         │                                                │
         │ Locks acquired                                 │
         │                                                │
         ▼                                                ▼
    ┌──────────┐                                     ┌──────────┐
    │EXECUTING │                                     │EXECUTING │
    │          │                                     │          │
    │Read A=$100│                                    │Read B=$50│
    │Write A=$0│                                     │Write B=$150│
    └────┬─────┘                                     └────┬─────┘
         │                                                │
         │ Local execution done                           │
         │                                                │
         ▼                                                ▼
    ┌──────────────────┐                         ┌──────────────────┐
    │ Send PREPARE_OK  │────────────────────────→│ Collect votes    │
    │ to coordinator   │                         │ (coordinator     │
    │ (Shard 2)        │                         │  shard)          │
    └──────────────────┘                         └────┬─────────────┘
                                                      │
                                                      │ All shards OK?
                                                      │
                                                      ▼
                                                 ┌─────────────┐
                                                 │   DECIDE:   │
                                                 │   COMMIT    │
                                                 └──────┬──────┘
                                                        │
                                   Broadcast COMMIT to all shards
                                                        │
         ┌──────────────────────────────────────────────┴────────┐
         ▼                                                        ▼
    ┌──────────┐                                             ┌──────────┐
    │  COMMIT  │                                             │  COMMIT  │
    │          │                                             │          │
    │Apply A=$0│                                             │Apply B=$150│
    └────┬─────┘                                             └────┬─────┘
         │                                                        │
         │ Release lock on A                                      │
         │                                                        │
         ▼                                                        ▼
    Move to seq=5001                                         Move to seq=5001

┌──────────────────────────────────────────────────────────────────────────┐
│                    WHY NO 2PC NEEDED?                                     │
└──────────────────────────────────────────────────────────────────────────┘

Traditional 2PC problem:
  - Shard 1 commits, Shard 2 crashes before commit
  - Inconsistent state: A debited, B not credited

Calvin's deterministic execution:
  - ALL shards execute in SAME order
  - If Shard 2 crashes, it will replay from sequence log
  - When Shard 2 recovers:
    - Reads sequence log
    - Sees T(seq=5000)
    - Re-executes deterministically
    - Arrives at same COMMIT decision
  - Determinism ensures consistency!

Edge case - What if execution fails on one shard?
  Example: Insufficient balance on Shard 1

  Shard 1                                 Shard 2
     │                                       │
     ▼                                       ▼
  Execute: balance_A = $10 (not $100!)   Execute: OK
     │                                       │
     ▼                                       ▼
  Decide: ABORT (insufficient funds)     Collect votes
     │                                       │
     └──────── Send ABORT vote ─────────────→│
                                              │
                                              ▼
                                         All shards:
                                         ABORT

  Because execution is deterministic:
  - All replicas of Shard 1 see balance_A = $10
  - All replicas decide ABORT
  - All replicas of Shard 2 receive ABORT
  - Consistent outcome!
```

---

## 6. Failure Scenarios & Recovery

```
┌──────────────────────────────────────────────────────────────────────────┐
│                    FAILURE SCENARIO 1: Sequencer Leader Crash             │
└──────────────────────────────────────────────────────────────────────────┘

Time ───────────────────────────────────────────────────────────────────→

         Sequencer 1 (Leader)   Sequencer 2 (Follower)   Sequencer 3 (Follower)
              │                         │                         │
              │                         │                         │
    T=0       │ Propose Batch 1         │                         │
              ├────────────────────────→│                         │
              ├─────────────────────────┼────────────────────────→│
              │                         │                         │
    T=1       │                         │ Accept Batch 1          │ Accept Batch 1
              │←────────────────────────┤                         │
              │←────────────────────────┼─────────────────────────┤
              │                         │                         │
    T=2       │ Commit Batch 1          │                         │
              ├────────────────────────→│                         │
              ├─────────────────────────┼────────────────────────→│
              │                         │                         │
    T=3       │ Propose Batch 2         │                         │
              ├────────────────────────→│                         │
              ├─────────────────────────┼────────────────────────→│
              │                         │                         │
    T=4       ✗ CRASH!                  │                         │
                                        │                         │
    T=5                                 │ Heartbeat timeout       │ Heartbeat timeout
                                        │                         │
    T=6                                 │ Start leader election   │
                                        ├────────────────────────→│
                                        │ Vote request            │
                                        │                         │
    T=7                                 │←────────────────────────┤
                                        │ Vote granted            │
                                        │                         │
    T=8                                 │ I AM LEADER (Seq 2)     │
                                        ├────────────────────────→│
                                        │                         │
    T=9                                 │ Re-propose Batch 2      │
                                        │ (was in-flight)         │
                                        ├────────────────────────→│
                                        │                         │
    T=10                                │←────────────────────────┤
                                        │ Accepted                │
                                        │                         │
    T=11                                │ Commit Batch 2          │
                                        ├────────────────────────→│
                                        │                         │
                                        │ Resume normal operation │

Recovery guarantees:
  ✓ No batch lost (Paxos ensures committed batches are durable)
  ✓ No duplicate batches (sequence numbers are unique)
  ✓ Clients may need to retry in-flight requests (transparent to them)

┌──────────────────────────────────────────────────────────────────────────┐
│                    FAILURE SCENARIO 2: Shard Replica Crash                │
└──────────────────────────────────────────────────────────────────────────┘

Shard 1, Replica A       Shard 1, Replica B       Shard 1, Replica C
     │                          │                          │
     │                          │                          │
     │ Batch 1 (seq 0-99)       │                          │
     ├─────────────────────────→│                          │
     ├──────────────────────────┼─────────────────────────→│
     │                          │                          │
     │ Execute seq 0-99         │ Execute seq 0-99         │ Execute seq 0-99
     │ State: X=10, Y=20        │ State: X=10, Y=20        │ State: X=10, Y=20
     │                          │                          │
     │ Batch 2 (seq 100-199)    │                          │
     ├─────────────────────────→│                          │
     ├──────────────────────────┼─────────────────────────→│
     │                          │                          │
     │ Execute seq 100-149      │ Execute seq 100-149      │ Execute seq 100-120
     │                          │                          ✗ CRASH!
     │                          │
     │ State: X=15, Y=25        │ State: X=15, Y=25
     │                          │
     │                          │                          │ REBOOT
     │                          │                          │
     │                          │←─────────────────────────┤
     │                          │  "What's my state?"      │
     │                          │                          │
     │                          │──────────────────────────→│
     │                          │  "You're at seq 120,     │
     │                          │   need to catch up"      │
     │                          │                          │
     │                          │──────────────────────────→│
     │                          │  Send state snapshot:    │
     │                          │  X=15, Y=25              │
     │                          │                          │
     │                          │──────────────────────────→│
     │                          │  Send missing batches:   │
     │                          │  Batch 2 (seq 121-199)   │
     │                          │                          │
     │                          │                          │ Replay 121-199
     │                          │                          │ State: X=15, Y=25
     │                          │                          │
     │ Batch 3 (seq 200-299)    │                          │
     ├─────────────────────────→│                          │
     ├──────────────────────────┼─────────────────────────→│
     │                          │                          │
     │ Resume normal operation  │                          │ Caught up!

Recovery mechanisms:
  1. Snapshot transfer (state at sequence N)
  2. Log replay (batches from sequence N+1 to current)
  3. Checksum verification (ensure consistency)

┌──────────────────────────────────────────────────────────────────────────┐
│                    FAILURE SCENARIO 3: Network Partition                  │
└──────────────────────────────────────────────────────────────────────────┘

Scenario: Partition splits sequencers into [1,2] and [3]

    Sequencer 1 (Leader)  Sequencer 2     │  Sequencer 3
         │                     │           │       │
         │                     │           │       │
    T=0  │ Propose Batch N     │           │       │
         ├────────────────────→│           │       │
         │                     │           │   ✗ Partition!
         ├─────────────────────┼───────────╫──────→✗
         │                     │           │       │
    T=1  │←────────────────────┤           │       │
         │ Accepted            │           │       │
         │                     │           │       │ Timeout waiting
         │                     │           │       │ for proposal
         │                     │           │       │
    T=2  │ Commit Batch N      │           │       │
         │ (quorum: 2/3 ✓)     │           │       │
         ├────────────────────→│           │       │
         │                     │           │       │
         │ Continue with       │           │       │ Start election
         │ Batch N+1           │           │       │ (but only 1/3)
         │                     │           │       │ Can't get quorum
         │                     │           │       │ → Stay as follower
         │                     │           │       │
    Partition heals at T=10             │
         │                     │           │       │
         │ Heartbeat reaches   │           │       │
         ├─────────────────────┼───────────────────→│
         │                     │           │       │
         │                     │           │       │ Catch-up request
         │←────────────────────┼───────────────────┤
         │                     │           │       │
         │ Send missing batches│           │       │
         ├─────────────────────┼───────────────────→│
         │ (Batch N, N+1, ...) │           │       │
         │                     │           │       │
         │                     │           │       │ Replay & sync
         │                     │           │       │
         │ Resume normal operation           │

Result: Majority partition (1,2) continues, minority (3) waits
        → No split-brain, consistency maintained
```

---

## 7. Performance Optimization: Reconstitution

```
┌──────────────────────────────────────────────────────────────────────────┐
│          RECONSTITUTION: Execute Reads Before Sequencing                  │
└──────────────────────────────────────────────────────────────────────────┘

Problem: Latency from waiting in batch + Paxos + execution

Traditional flow:
    Client → Sequencer → Paxos → Shard → Execute reads + writes → Client
             └─ 5ms ──┘ └ 1ms ┘         └─────── 2ms ─────────┘
                         Total: ~8ms

Optimization: Reconstitution (for deterministic read sets)

Improved flow:
    Client ──→ Shard (execute READS immediately)
       │            │
       │            └──→ Return read results to client
       │                 (but don't commit yet)
       │
       └──→ Sequencer → Paxos → Shard (execute WRITES with read set)
                └─ 5ms ──┘ └1ms┘      └── 1ms ──┘
                         Total: ~7ms
                         Client saw results earlier!

Detailed flow:

┌──────────────────────────────────────────────────────────────────────────┐
│   Step 1: Client sends transaction with known read set                   │
└──────────────────────────────────────────────────────────────────────────┘

    Client: "Transfer $50 from A to B"
    Read set: {A, B} (known statically)
    Write set: {A, B} (known statically)

    Client submits to Shard 1 (home shard)

┌──────────────────────────────────────────────────────────────────────────┐
│   Step 2: Shard executes reads speculatively                             │
└──────────────────────────────────────────────────────────────────────────┘

    Shard 1:
      1. Take snapshot at current committed sequence N
      2. Read A.balance = $100 at snapshot N
      3. Read B.balance = $50 at snapshot N
      4. Return to client: "A=$100, B=$50"
      5. Buffer transaction: T_pending{read_values: {A:$100, B:$50}}

┌──────────────────────────────────────────────────────────────────────────┐
│   Step 3: Client computes writes, sends to sequencer                     │
└──────────────────────────────────────────────────────────────────────────┘

    Client:
      1. Compute: A_new = $100 - $50 = $50
      2. Compute: B_new = $50 + $50 = $100
      3. Send to sequencer: T{writes: {A:$50, B:$100}, read_snapshot: N}

┌──────────────────────────────────────────────────────────────────────────┐
│   Step 4: Sequencer assigns sequence number                              │
└──────────────────────────────────────────────────────────────────────────┘

    Sequencer:
      1. Add to batch
      2. Seal batch, run Paxos
      3. Assign sequence number: seq = 1000
      4. Forward to shards: T{seq:1000, writes:{A:$50, B:$100}, snapshot:N}

┌──────────────────────────────────────────────────────────────────────────┐
│   Step 5: Shard validates and commits                                    │
└──────────────────────────────────────────────────────────────────────────┘

    Shard 1:
      1. Wait for seq = 1000
      2. Validate: Have A and B changed since snapshot N?
         - If yes: ABORT (read set invalid)
         - If no: Proceed
      3. Acquire locks on A and B
      4. Apply writes: A=$50, B=$100
      5. Commit at seq=1000

┌──────────────────────────────────────────────────────────────────────────┐
│   Benefits                                                                │
└──────────────────────────────────────────────────────────────────────────┘

    1. Client sees results faster (during batching window)
    2. Sequencer has less work (no reads, only writes)
    3. Better resource utilization (pipelining)

┌──────────────────────────────────────────────────────────────────────────┐
│   Limitations                                                             │
└──────────────────────────────────────────────────────────────────────────┘

    1. Only works for deterministic read sets (known upfront)
    2. Abort rate may increase (if reads become stale)
    3. Requires client-side logic (compute writes)

    Best for: Read-heavy transactions, low contention keys
    Worst for: Write-heavy, high contention, dynamic read sets
```

---

## 8. Throughput Scaling: Batch Size vs Latency

```
┌──────────────────────────────────────────────────────────────────────────┐
│                BATCH SIZE VS LATENCY TRADE-OFF                            │
└──────────────────────────────────────────────────────────────────────────┘

Throughput = Batch Size / Batch Window

                    Latency (ms)
                    │
                 50 │                              ╱
                    │                            ╱
                 40 │                          ╱
                    │                        ╱
                 30 │                      ╱
                    │                    ╱
                 20 │                  ╱
                    │                ╱
                 10 │              ╱
                    │            ╱
                  0 │──────────╱─────────────────────────────────
                    0         10K        100K       1M      10M
                           Throughput (TPS)

Configurations:

┌─────────────────┬──────────────┬──────────────┬──────────────┐
│ Batch Window    │ Batch Size   │ Throughput   │ Avg Latency  │
├─────────────────┼──────────────┼──────────────┼──────────────┤
│ 1ms             │ 1,000        │ 1M TPS       │ 1.5ms        │
│ 5ms             │ 5,000        │ 1M TPS       │ 7.5ms        │
│ 10ms            │ 10,000       │ 1M TPS       │ 15ms         │
│ 50ms            │ 50,000       │ 1M TPS       │ 75ms         │
└─────────────────┴──────────────┴──────────────┴──────────────┘

┌─────────────────┬──────────────┬──────────────┬──────────────┐
│ Batch Window    │ Batch Size   │ Throughput   │ Avg Latency  │
├─────────────────┼──────────────┼──────────────┼──────────────┤
│ 10ms            │ 100          │ 10K TPS      │ 15ms         │
│ 10ms            │ 1,000        │ 100K TPS     │ 15ms         │
│ 10ms            │ 10,000       │ 1M TPS       │ 15ms         │
│ 10ms            │ 100,000      │ 10M TPS      │ 15ms         │
└─────────────────┴──────────────┴──────────────┴──────────────┘

Insights:
  - Latency dominated by batch window (waiting time)
  - Throughput scales linearly with batch size (up to CPU limits)
  - Sweet spot: 10ms window, 10K-50K batch size
  - For low-latency: 1-5ms window, sacrifice some throughput

Adaptive batching algorithm:

    if queue_depth > 10,000:
        batch_window = 1ms    # High load → reduce latency
    elif queue_depth > 1,000:
        batch_window = 5ms    # Medium load → balance
    else:
        batch_window = 10ms   # Low load → maximize throughput
```

---

## 9. Geographic Replication Architecture

```
┌──────────────────────────────────────────────────────────────────────────┐
│               MULTI-REGION DEPLOYMENT                                     │
└──────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────┐
│                         REGION: US-EAST                                 │
│                                                                         │
│   Client ──→ Sequencer 1 (Leader) ──→ Shard 1A, 2A, 3A                 │
│                    │                                                    │
│                    │ Paxos replication                                  │
│                    ├──────────────────────────────────────┐             │
└────────────────────┼──────────────────────────────────────┼─────────────┘
                     │                                      │
                     │ WAN latency: 50-100ms                │
                     │                                      │
┌────────────────────▼──────────────────────────────────────┼─────────────┐
│                         REGION: EU-WEST                    │             │
│                                                            │             │
│   Client ──→ Sequencer 2 (Follower) ──→ Shard 1B, 2B, 3B  │             │
│                                                            │             │
└────────────────────────────────────────────────────────────┼─────────────┘
                                                             │
                                                             │
┌────────────────────────────────────────────────────────────▼─────────────┐
│                         REGION: ASIA-PACIFIC                             │
│                                                                           │
│   Client ──→ Sequencer 3 (Follower) ──→ Shard 1C, 2C, 3C                 │
│                                                                           │
└───────────────────────────────────────────────────────────────────────────┘

Replication flow:

    US-East Client                   EU Client                 Asia Client
         │                              │                          │
         │ Submit txn                   │                          │
         ▼                              │                          │
    Sequencer 1 (Leader)                │                          │
         │                              │                          │
         │ Batch + Paxos                │                          │
         ├─────────────────────────────→ Sequencer 2               │
         ├──────────────────────────────┼─────────────────────────→Sequencer 3
         │   (50ms WAN)                 │     (150ms WAN)          │
         │                              │                          │
         ▼                              ▼                          ▼
    Shards A                        Shards B                   Shards C
    (execute)                       (execute)                  (execute)

Latency breakdown for US-East client:
  - Batching: 5ms (avg)
  - Paxos (local): 1ms
  - Paxos (WAN): 50ms (wait for EU) + 150ms (wait for Asia)
  - Total: ~206ms

Optimization: Quorum in same region
  - Sequencer replicas: [US-East-1, US-East-2, US-East-3]
  - Paxos quorum: 2/3 in same region
  - WAN replication: Asynchronous (eventual consistency)
  - Latency: ~7ms for local clients
  - Trade-off: Weaker durability guarantee (1 region can commit)

┌──────────────────────────────────────────────────────────────────────────┐
│               HYBRID: Local + Global Sequencing                           │
└──────────────────────────────────────────────────────────────────────────┘

Partition transactions by scope:

LOCAL transactions (single region):
  - Sequenced by local sequencer group
  - Fast path: 7ms latency
  - Example: "Bob transfers $50 to Alice" (both in US)

GLOBAL transactions (cross-region):
  - Sequenced by global sequencer group
  - Slow path: 200ms latency
  - Example: "Bob (US) transfers $50 to Alice (EU)"

Detection:
  - Client declares: local or global
  - Or system infers from data location (home region of keys)

Benefits:
  - 95% of transactions are local → low latency
  - 5% global transactions → high latency but correct
  - Better UX than always waiting for WAN
```

---

## Summary

This document provides detailed diagrams for:

1. **Transaction State Machine**: How transactions progress through sequencing
2. **Sequencer Internals**: Batching, Paxos, and distribution logic
3. **Shard Execution**: Deterministic ordering and lock management
4. **Lock Queues**: How locks are acquired in sequence order
5. **Cross-Shard Protocols**: Coordination without 2PC
6. **Failure Recovery**: Handling sequencer/shard crashes and partitions
7. **Reconstitution**: Optimization for read-heavy workloads
8. **Throughput Scaling**: Batch size vs latency trade-offs
9. **Geographic Replication**: Multi-region deployment strategies

These diagrams should help understand how distributed sequencing provides:
- ✅ Deterministic execution across replicas
- ✅ Fault tolerance via Paxos
- ✅ High throughput via batching (1M+ TPS)
- ✅ Consistency without 2PC
- ✅ Scalability across regions

For implementation details, refer to `DISTRIBUTED_SEQUENCING_PLAN.md`.
