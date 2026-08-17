# GoDDB: Distributed Key-Value Database

GoDDB is a highly available, fault-tolerant distributed key-value database built from scratch in Go. It combines the **Raft Consensus Algorithm** for distributed consensus with an **LSM-Tree** (Log-Structured Merge-Tree) storage engine for persistent, high-throughput storage.

![Go](https://img.shields.io/badge/go-%2300ADD8.svg?style=for-the-badge&logo=go&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-244C5A.svg?style=for-the-badge&logo=grpc&logoColor=white)
![Protobuf](https://img.shields.io/badge/Protocol%20Buffers-4285F4.svg?style=for-the-badge&logo=google&logoColor=white)

---

## 📖 Table of Contents
- [Architecture Overview](#-architecture-overview)
- [Project Structure](#-project-structure)
- [Deep Dive: Raft Consensus Layer](#-deep-dive-raft-consensus-layer)
- [Deep Dive: LSM-Tree Storage Engine](#-deep-dive-lsm-tree-storage-engine)
- [Getting Started](#%EF%B8%8F-getting-started)
- [Fault Tolerance & Resilience Testing](#-fault-tolerance--resilience-testing)

---

## 🚀 Architecture Overview

GoDDB implements modern database and distributed systems patterns:

1. **Raft Consensus Layer (`raft/`)**: Ensures strong consistency and high availability across cluster nodes. If the Leader fails, remaining nodes elect a new Leader in milliseconds without data loss or split-brains.
2. **LSM-Tree Storage Engine (`engine/`)**: Optimizes write throughput by appending mutations to an in-memory AVL Tree (MemTable) backed by a Write-Ahead Log (WAL), flushing sequentially to immutable Sorted String Tables (SSTables) on disk.
3. **gRPC Network Layer (`server/`, `proto/`)**: Handles all inter-node consensus RPCs (`RequestVote`, `AppendEntries`, `InstallSnapshot`, `JoinCluster`) and client requests over low-latency gRPC.

---

## 🏗️ Project Structure

```mermaid
graph TD
    Client["Client CLI (client/)"] -- gRPC --> Server["Server Transport (server/)"]
    Server -- Proposes Write --> Raft["Raft Consensus (raft/)"]
    
    subgraph Raft Consensus Cluster
        Raft -- "1. Parallel Log Replication" --> OtherNodes["Follower Nodes"]
        OtherNodes -- "2. Quorum ACK" --> Raft
    end
    
    Raft -- "3. Async Commit" --> Engine["LSM Storage Engine (engine/)"]
    
    subgraph LSM Storage Engine (engine/)
        Engine -- "A. Appends to" --> WAL["Write-Ahead Log (engine/wal/)"]
        Engine -- "B. Inserts into" --> MemTable["MemTable / AVL Tree (engine/memtable/)"]
        MemTable -- "C. Flushes to disk" --> SSTable["SSTables (engine/sstable/)"]
        SSTable -. "Accelerates reads via" .-> Bloom["Bloom Filters (engine/bloom/)"]
    end
```

### Directory Breakdown

```
├── engine/                 # LSM-Tree Storage Engine Domain
│   ├── engine.go           # Engine interface definition
│   ├── memory.go           # Storage engine orchestrator & recovery manager
│   ├── bloom/              # Probabilistic Bloom Filter for skipping disk reads
│   │   └── bloom.go
│   ├── memtable/           # In-memory balanced AVL Tree index
│   │   └── avl.go
│   ├── sstable/            # Immutable on-disk Sorted String Tables
│   │   └── sstable.go
│   └── wal/                # Write-Ahead Log durability layer
│       ├── entry.go
│       └── wal.go
├── raft/                   # Distributed Consensus Layer
│   └── node.go             # Raft state machine, replication, & persistence
├── server/                 # Network Transport Layer
│   └── server.go           # gRPC service implementation
├── proto/                  # Protocol Buffers schema & generated stubs
│   ├── db.proto
│   ├── db.pb.go
│   └── db_grpc.pb.go
├── client/                 # Interactive CLI client
│   └── main.go
├── main.go                 # Server node entrypoint
├── go.mod / go.sum
└── README.md
```

---

## ⚙️ Deep Dive: Raft Consensus Layer

The consensus engine in [`raft/node.go`](raft/node.go) is built for high throughput and non-blocking operation:

### 1. Dedicated Per-Peer Replication Pipelines
Instead of a single blocking loop, the Leader runs an independent `peerReplicator` goroutine for each follower. Slower or temporarily partitioned nodes cannot stall consensus or increase write latency for healthy peers.

### 2. Instant Replication & Decoupled Heartbeats
- **Instant Event-Driven Replication**: When `Propose()` appends a log entry, it broadcasts to `replicateCond`, waking all peer goroutines immediately.
- **Strict Heartbeat Timer**: A background timer fires every 50ms to emit empty `AppendEntries` heartbeats, preventing election timeouts even during intensive write spikes.

### 3. Asynchronous Commit Applier
Advancing `commitIndex` queues committed entries to a buffered channel (`applyChannel`). A dedicated background worker (`runApplier`) executes writes against the storage engine without holding the main consensus mutex during disk I/O.

### 4. Non-Blocking Batched State Persistence
Consensus metadata changes signal a background worker (`runStatePersister`) via `persistDirty`. State changes (`state.json`) are batched and written to disk without blocking the critical RPC path.

### 5. Snapshot Catch-up Mechanism
When a follower falls behind compacted log entries, the Leader streams an SSTable snapshot (`data_compact.sst`) via `InstallSnapshot` to bring the follower up to date.

### 6. Dynamic Membership (`JOIN`)
Nodes can dynamically join a live cluster at runtime via the `JOIN` command. The Leader proposes the configuration change through the Raft log, and all nodes dial the new peer.

---

## 🗄️ Deep Dive: LSM-Tree Storage Engine

The storage layer in [`engine/`](engine/) delivers high write throughput by ensuring all disk operations are sequential:

### 1. MemTable (AVL Tree)
All `PUT` and `DELETE` operations update an in-memory balanced AVL Tree, providing $O(\log n)$ reads and writes without disk head movement.

### 2. Write-Ahead Log (WAL)
Before updating memory, every operation is written to the WAL. If a node crashes, `Recovery()` replays the WAL to reconstruct the active MemTable. Once flushed to an SSTable, the WAL is truncated.

### 3. Immutable SSTables & Compaction
When the MemTable reaches 10 keys, it flushes sorted keys to an immutable `.sst` file. A background compaction process merges older SSTables, purges tombstones, and produces a consolidated `data_compact.sst`.

### 4. Bloom Filters
Each SSTable maintains an in-memory Bloom filter. If a key is not present in the filter, disk I/O for that SSTable is bypassed entirely.

---

## 🛠️ Getting Started

### Prerequisites
- **Go 1.20+**
- **Protocol Buffers Compiler (`protoc`)** (only if editing `.proto` files)

### 1. Start a 3-Node Local Cluster

Open 3 separate terminals:

**Terminal 1 (Seed Node):**
```bash
go run main.go 50051
```

**Terminal 2 (Node 2 - Join Seed):**
```bash
go run main.go 50052 50051
```

**Terminal 3 (Node 3 - Join Seed):**
```bash
go run main.go 50053 50051
```

---

### 2. Connect the Interactive CLI

Open a new terminal to connect to the cluster:

```bash
cd client
go run main.go 50051
```

### 3. Database Commands

```bash
# Insert / update key-value pairs
ddb> PUT user:100 "Test"
OK

# Retrieve a value by key
ddb> GET user:100
"Dev Rao"

# Delete a key (writes a tombstone)
ddb> DELETE user:100
OK

# Dynamically add a new node (e.g. port 50054) to the live cluster
ddb> JOIN 50054
OK

# Exit the CLI
ddb> exit
```

---

## 🛡️ Fault Tolerance & Resilience Testing

### Test 1: Leader Failover
1. Boot a 3-node cluster (`50051`, `50052`, `50053`).
2. Kill the Leader terminal with `Ctrl+C`.
3. Within milliseconds, the remaining followers detect the timeout, elect a new Leader, and resume serving client reads and writes.

### Test 2: Follower Recovery & Snapshot Streaming
1. Kill a Follower node.
2. Issue 20+ `PUT` commands to the Leader to trigger log compaction.
3. Restart the killed Follower.
4. The Follower restores its term and peers from `state.json`, detects it is behind, and receives an `InstallSnapshot` from the Leader to restore the full dataset.
