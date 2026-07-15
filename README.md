# GoDDB: Distributed Key-Value Database

GoDDB is a highly available, fault-tolerant distributed key-value database built entirely from scratch in Go. It implements the **Raft Consensus Algorithm** for distributed state management and an **LSM-Tree** (Log-Structured Merge-Tree) storage engine for extremely fast, persistent data storage.

![Go](https://img.shields.io/badge/go-%2300ADD8.svg?style=for-the-badge&logo=go&logoColor=white)
![gRPC](https://img.shields.io/badge/gRPC-244C5A.svg?style=for-the-badge&logo=grpc&logoColor=white)
![Protobuf](https://img.shields.io/badge/Protocol%20Buffers-4285F4.svg?style=for-the-badge&logo=google&logoColor=white)

## 📖 Table of Contents
- [Architecture Overview](#-architecture-overview)
- [Deep Dive: LSM-Tree Storage Engine](#-deep-dive-lsm-tree-storage-engine)
- [Deep Dive: Raft Consensus Layer](#-deep-dive-raft-consensus-layer)
- [Getting Started](#%EF%B8%8F-getting-started)
- [Fault Tolerance & Testing](#-fault-tolerance--testing)

---

## 🚀 Architecture Overview

GoDDB combines the core principles used in modern production databases (like Cassandra, RocksDB, and etcd):

1. **Raft Consensus Layer:** Guarantees strong consistency and high availability across the cluster. If the Leader crashes, the cluster seamlessly elects a new Leader in milliseconds. It perfectly handles split-brains, network partitions, and node failures.
2. **LSM-Tree Storage Engine:** Instead of updating data in place, all writes are appended to an in-memory AVL Tree (MemTable) and periodically flushed to immutable Sorted String Tables (SSTables) on the hard drive.
3. **gRPC Network Layer:** All inter-node communication (Heartbeats, Leader Elections, and Snapshots) and client-server communication runs on high-performance gRPC.

---

## 🗄️ Deep Dive: LSM-Tree Storage Engine

The storage layer is engineered for extremely high write throughput. It bypasses the random I/O bottleneck of traditional B-Tree databases by ensuring that all disk writes are strictly sequential.

### 1. MemTable (AVL Tree)
When a `PUT` or `DELETE` request arrives, the data is immediately inserted into an in-memory balanced AVL tree. This ensures `O(log n)` time complexity for all read and write operations. Because data is structured in memory, it can be instantly retrieved without touching the disk.

### 2. Write-Ahead Log (WAL)
To prevent data loss if the server crashes before the MemTable is saved, every operation is simultaneously appended to a sequential disk file called the Write-Ahead Log. Upon reboot, the database replays the WAL to perfectly reconstruct the MemTable.

### 3. Sorted String Tables (SSTables)
Once the MemTable reaches a predefined capacity, the database locks the table, flushes the sequential keys to an immutable `data.sst` file on the disk, and clears the memory. 

### 4. Bloom Filters
Searching through dozens of SSTable files on disk for a single key is incredibly slow. To solve this, every SSTable generates a **Bloom Filter** (a probabilistic data structure). The Bloom Filters are kept in RAM. Before checking an SSTable, the database queries the Bloom Filter. If it returns `false`, the database knows with 100% certainty that the key does not exist in that file, instantly skipping the disk read!

### 5. Compaction
Over time, keys may be updated or deleted, leaving stale data in older SSTables. A background process periodically reads multiple SSTables, merges them together, purges deleted/stale keys, and writes a newly compacted SSTable.

---

## ⚙️ Deep Dive: Raft Consensus Layer

GoDDB implements a strict, ground-up version of the Raft consensus algorithm. It ensures that every node in the cluster holds the exact same data, and that the database remains highly available even if multiple servers catch on fire.

### 1. Leader Election
Nodes start as `Followers` with randomized countdown timers. If they don't receive a heartbeat from a Leader before their timer expires, they promote themselves to `Candidate`, increment their Term, and request votes from the cluster. The first node to receive a majority of votes becomes the `Leader`. Randomized timers mathematically prevent "split votes" from deadlocking the system.

### 2. Log Replication & Strict Safety
The Leader accepts client requests, appends them to its Raft Log, and broadcasts them to the Followers via `AppendEntries` RPCs. It only commits the data to the LSM-Tree after a majority of Followers acknowledge the log. 
**Safety First:** GoDDB implements the Raft `Up-To-Date` restriction. A Follower will absolutely refuse to vote for a Candidate if the Candidate's log is older than its own. This guarantees that a node missing data can *never* become the Leader.

### 3. Auto-Snapshotting (Catch-up Mechanism)
If a node goes offline and misses thousands of logs, the Leader's in-memory log will have already been compacted. Instead of failing, the Leader automatically reads its compacted `data_compact.sst` and streams a massive state snapshot over gRPC to the lagging Follower, instantly syncing it back into the cluster.

### 4. Dynamic Cluster Membership
You do not need to restart the cluster to scale it! You can add new nodes dynamically by issuing a `JOIN` command. The Leader processes this configuration change as a standard log entry, and all nodes dynamically open background gRPC connections to the new server on the fly.

### 5. Consensus State Persistence
A dedicated `state.json` file on the disk permanently stores the node's `CurrentTerm`, `VotedFor`, and active `Peers`. If a node crashes and reboots, it perfectly remembers the cluster topology. This completely prevents "Amnesia Loops" where a rebooted node mistakenly believes it is alone and triggers rogue elections.

---

## 🛠️ Getting Started

### Prerequisites

- Go 1.20+
- Protocol Buffers Compiler (`protoc`)

### 1. Start the Cluster

You can run the database nodes locally on different ports. Each node will create its own isolated `files/node_{port}` directory for storage.

Open Terminal 1 (Start the Seed Node):

```bash
go run main.go 50051
```

_Wait a few seconds for it to win the election and become the Leader._

Open Terminal 2 (Start Node 2 and join the cluster):

```bash
go run main.go 50052 50051
```

Open Terminal 3 (Start Node 3 and join the cluster):

```bash
go run main.go 50053 50051
```

### 2. Connect the Client CLI

Open a new terminal and connect the CLI to the Leader node:

```bash
cd client
go run main.go 50051
```

### 3. Interact with the Database

Inside the interactive CLI, you can run:

```bash
# Store data
> PUT username xyz
OK

# Retrieve data
> GET username
xyz

# Delete data
> DELETE username
OK

# Dynamically add a new node to the live cluster
> JOIN 50054
OK
```

---

## 🛡️ Fault Tolerance & Testing

GoDDB is built to survive chaos. You can manually test its resilience by simulating catastrophic failures:

### Test 1: Leader Assassination
1. Boot a 3-node cluster.
2. Go to the terminal running the Leader (usually `50051`) and hit `Ctrl+C` to kill it.
3. Watch the other terminals! Within milliseconds, their timers will expire, they will hold an election, and a new Leader will be crowned to keep the database online.

### Test 2: Split Brain & Amnesia Recovery
1. Boot a 3-node cluster.
2. Kill a Follower node.
3. Wait 10 seconds, then reboot the Follower node.
4. Watch as the Follower instantly reads its `state.json`, perfectly remembers the cluster, seamlessly reconnects to the existing Leader, and requests an `InstallSnapshot` to download all the data it missed while it was offline!
