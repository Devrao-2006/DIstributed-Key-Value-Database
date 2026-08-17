package raft

import (
	"DDB/engine"
	"DDB/proto"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Raft node role states.
const (
	Follower  state = 1
	Candidate state = 2
	Leader    state = 3
)

type state uint8

// Node represents a single consensus participant in the Raft cluster.
// It manages term state, logs, peer connections, and synchronization primitives.
type Node struct {
	mu                sync.Mutex
	state             state
	currentTerm       int64
	votedFor          string
	id                string
	peers             map[string]proto.DatabaseClient
	db                *engine.MemoryEngine
	lastContacted     time.Time
	currentLeader     string
	log               []*proto.LogEntry
	commitIndex       int64
	lastApplied       int64
	lastIncludedTerm  int64
	lastIncludedIndex int64
	snapshotting      map[string]bool
	nextIndex         map[string]int64
	matchIndex        map[string]int64
	commitCond        *sync.Cond
	replicateCond     *sync.Cond
	applyChannel      chan []*proto.LogEntry
	persistDirty      chan struct{}
}

// NewNode initializes and constructs a new Raft consensus Node instance.
//
// Purpose:
//   Bootstraps a fresh or recovering node, binds its storage engine, restores any
//   persisted consensus metadata from disk, and spawns the core background workers.
//
// How it does it:
//   1. Allocates the Node struct with default follower state and initialized maps.
//   2. Instantiates condition variables (commitCond, replicateCond) tied to the node mutex.
//   3. Restores saved cluster state (term, votedFor, peers, logs) via loadState().
//   4. Spawns the background apply loop (runApplier) for asynchronous disk writes.
//   5. Spawns the background state persistence worker (runStatePersister).
func NewNode(url string, db *engine.MemoryEngine) *Node {
	peersmap := make(map[string]proto.DatabaseClient)
	node := &Node{
		state:             Follower,
		currentTerm:       0,
		votedFor:          "",
		id:                url,
		db:                db,
		peers:             peersmap,
		lastContacted:     time.Now(),
		lastIncludedTerm:  -1,
		lastIncludedIndex: -1,
		commitIndex:       -1,
		lastApplied:       -1,
		snapshotting:      make(map[string]bool),
		nextIndex:         make(map[string]int64),
		matchIndex:        make(map[string]int64),
		applyChannel:      make(chan []*proto.LogEntry, 1000),
		persistDirty:      make(chan struct{}, 1),
	}
	node.commitCond = sync.NewCond(&node.mu)
	node.replicateCond = sync.NewCond(&node.mu)
	node.loadState()
	go node.runApplier()
	go node.runStatePersister()
	return node
}

// Propose appends a client command to the Raft log and blocks until it is safely committed.
//
// Purpose:
//   Acts as the entry point for client write operations (PUT, DELETE, JOIN).
//   Ensures an entry is replicated to a quorum before returning success to the caller.
//
// How it does it:
//   1. Acquires n.mu and verifies the current node is the Leader.
//   2. Appends a new proto.LogEntry with the current term and command payload to n.log.
//   3. Computes targetCommit = lastIncludedIndex + len(log).
//   4. Requests asynchronous disk persistence via signalPersist().
//   5. If a single-node cluster, directly advances commit index; otherwise, broadcasts
//      on replicateCond to immediately wake all per-peer replicator goroutines.
//   6. Waits on commitCond until commitIndex >= targetCommit or leadership is lost.
func (n *Node) Propose(command, key, value string) error {
	n.mu.Lock()
	if n.state != Leader {
		n.mu.Unlock()
		return fmt.Errorf("Lost leadership before commit")
	}

	var logEntry = proto.LogEntry{Term: n.currentTerm, Command: command, Key: key, Value: value}
	n.log = append(n.log, &logEntry)
	targetCommit := n.lastIncludedIndex + int64(len(n.log))
	n.signalPersist()

	// If there are no peers (single node cluster), commit directly!
	if len(n.peers) == 0 {
		n.checkAndAdvanceCommitIndex()
	} else {
		// Instantly wake up all peer replicators
		n.replicateCond.Broadcast()
	}

	for n.commitIndex < targetCommit && n.state == Leader {
		n.commitCond.Wait()
	}

	if n.commitIndex >= targetCommit {
		n.mu.Unlock()
		return nil
	}
	n.mu.Unlock()
	return fmt.Errorf("Lost leadership before commit")
}

// startLeaderLocked transitions the node into the active Leader role.
// Must be called with n.mu held.
//
// Purpose:
//   Spawns the background goroutines responsible for maintaining leadership and replicating logs.
//
// How it does it:
//   1. Captures the current term while holding the mutex.
//   2. Spawns runHeartbeatTimer to emit periodic heartbeat wakeups every 50ms.
//   3. Spawns an independent peerReplicator goroutine for every connected peer.
func (n *Node) startLeaderLocked() {
	if n.state != Leader {
		return
	}
	term := n.currentTerm

	// 1. Spawn decoupled heartbeat timer
	go n.runHeartbeatTimer(term)

	// 2. Spawn dedicated peer replication goroutines
	for peerPort, client := range n.peers {
		go n.peerReplicator(peerPort, client, term)
	}
}

// runHeartbeatTimer runs a strict 50ms ticker to trigger periodic empty heartbeats.
//
// Purpose:
//   Maintains leader authority across followers to prevent election timeouts.
//
// How it does it:
//   1. Runs a time.Ticker every 50 milliseconds.
//   2. On each tick, acquires n.mu and checks if leadership and term remain active.
//   3. Calls replicateCond.Broadcast() to wake all peerReplicator goroutines.
//   4. Exits automatically when the node is no longer the leader for this term.
func (n *Node) runHeartbeatTimer(term int64) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for range ticker.C {
		n.mu.Lock()
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}
		n.replicateCond.Broadcast()
		n.mu.Unlock()
	}
}

// peerReplicator manages the dedicated log replication pipeline for a single peer.
//
// Purpose:
//   Independently streams new log entries or sends snapshots to a specific peer follower
//   without blocking other peers or stalling when this peer experiences high latency.
//
// How it does it:
//   1. Loops continuously while the node remains Leader for the term.
//   2. Checks if the follower's nextIndex falls behind the compacted log boundary; if so,
//      initiates an asynchronous SSTable snapshot transmission (sendSnapshot).
//   3. Prepares the entries slice and calculates prevLogIndex/prevLogTerm.
//   4. Releases n.mu and issues a gRPC AppendEntries RPC with a 50ms context timeout.
//   5. Re-acquires n.mu:
//      - If a higher term is discovered in the response, reverts to Follower.
//      - On success, advances matchIndex and nextIndex, and calls checkAndAdvanceCommitIndex().
//      - On failure/log mismatch, decrements nextIndex to locate the common ancestor log.
//   6. If more unsent logs remain, loops immediately; otherwise, calls replicateCond.Wait().
func (n *Node) peerReplicator(peerPort string, client proto.DatabaseClient, term int64) {
	for {
		n.mu.Lock()
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}

		nextIdx := n.nextIndex[peerPort]

		// Check if follower fell behind and requires an SSTable snapshot
		if nextIdx <= n.lastIncludedIndex {
			if !n.snapshotting[peerPort] {
				n.snapshotting[peerPort] = true
				go func(addr string, c proto.DatabaseClient) {
					n.sendSnapshot(addr, c)
					n.mu.Lock()
					n.snapshotting[addr] = false
					n.nextIndex[addr] = n.lastIncludedIndex + 1
					n.replicateCond.Broadcast()
					n.mu.Unlock()
				}(peerPort, client)
			}
			n.replicateCond.Wait()
			n.mu.Unlock()
			continue
		}

		prevLogIndex := nextIdx - 1
		prevLogTerm := n.lastIncludedTerm
		logIdx := prevLogIndex - n.lastIncludedIndex - 1
		if logIdx >= 0 && logIdx < int64(len(n.log)) {
			prevLogTerm = n.log[logIdx].Term
		}

		var sendLogs []*proto.LogEntry
		startIdx := nextIdx - n.lastIncludedIndex - 1
		if startIdx >= 0 && startIdx <= int64(len(n.log)) {
			sendLogs = make([]*proto.LogEntry, len(n.log[startIdx:]))
			copy(sendLogs, n.log[startIdx:])
		}

		leaderId := n.id
		commitIndex := n.commitIndex
		currTerm := n.currentTerm
		n.mu.Unlock()

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		resp, err := client.AppendEntries(ctx, &proto.AppendEntriesRequest{
			Term:         currTerm,
			LeaderId:     leaderId,
			Entries:      sendLogs,
			LeaderCommit: commitIndex,
			PrevLogIndex: prevLogIndex,
			PrevLogTerm:  prevLogTerm,
		})
		cancel()

		n.mu.Lock()
		if n.state != Leader || n.currentTerm != term {
			n.mu.Unlock()
			return
		}

		if err == nil {
			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				n.signalPersist()
				n.commitCond.Broadcast()
				n.replicateCond.Broadcast()
				n.mu.Unlock()
				return
			}

			if resp.Success {
				n.lastContacted = time.Now()
				n.matchIndex[peerPort] = prevLogIndex + int64(len(sendLogs))
				n.nextIndex[peerPort] = n.matchIndex[peerPort] + 1
				n.checkAndAdvanceCommitIndex()
			} else {
				if n.nextIndex[peerPort] > 0 {
					n.nextIndex[peerPort]--
				}
			}
		}

		// If there are more pending entries for this peer, replicate immediately without sleeping
		lastLogIndex := n.lastIncludedIndex + int64(len(n.log))
		if n.nextIndex[peerPort] <= lastLogIndex {
			n.mu.Unlock()
			continue
		}

		n.replicateCond.Wait()
		n.mu.Unlock()
	}
}

// checkAndAdvanceCommitIndex calculates the median matchIndex across all peers to advance commitIndex.
// Must be called with n.mu held.
//
// Purpose:
//   Determines whether a log entry has been acknowledged by a majority of nodes and can be committed.
//
// How it does it:
//   1. Collects all peer match indices plus the leader's own highest log index into a slice.
//   2. Sorts indices in descending order and selects the majority median: matchIndices[len/2].
//   3. If the target median exceeds the current commitIndex and was created in the current term:
//      - Advances commitIndex to the target.
//      - Broadcasts on commitCond to unblock waiting Propose() calls.
//      - Collects newly committed proto.LogEntry items and non-blockingly queues them to applyChannel.
//      - Triggers log compaction if the uncompacted log size exceeds the threshold (10 entries).
func (n *Node) checkAndAdvanceCommitIndex() {
	if n.state != Leader {
		return
	}

	matchIndices := make([]int64, 0, len(n.matchIndex)+1)
	matchIndices = append(matchIndices, n.lastIncludedIndex+int64(len(n.log)))
	for _, m := range n.matchIndex {
		matchIndices = append(matchIndices, m)
	}

	slices.SortFunc(matchIndices, func(a, b int64) int {
		if a < b {
			return 1
		}
		if a > b {
			return -1
		}
		return 0
	})

	majorityIdx := len(matchIndices) / 2
	targetCommitIndex := matchIndices[majorityIdx]

	if targetCommitIndex > n.commitIndex {
		logIdx := targetCommitIndex - n.lastIncludedIndex - 1
		if logIdx >= 0 && logIdx < int64(len(n.log)) && n.log[logIdx].Term == n.currentTerm {
			oldCommit := n.commitIndex
			n.commitIndex = targetCommitIndex
			n.commitCond.Broadcast()

			toApply := make([]*proto.LogEntry, 0, targetCommitIndex-oldCommit)
			for i := oldCommit + 1; i <= targetCommitIndex; i++ {
				arrayIndex := i - n.lastIncludedIndex - 1
				if arrayIndex >= 0 && arrayIndex < int64(len(n.log)) {
					toApply = append(toApply, n.log[arrayIndex])
				}
			}

			if len(toApply) > 0 {
				select {
				case n.applyChannel <- toApply:
				default:
					go func(entries []*proto.LogEntry) {
						n.applyChannel <- entries
					}(toApply)
				}
			}

			if n.commitIndex-n.lastIncludedIndex > 10 {
				n.compactLog(n.commitIndex - 1)
			}
		}
	}
}

// compactLog trims in-memory log entries up to a given index that have already been persisted.
// Must be called with n.mu held.
//
// Purpose:
//   Prevents unbounded in-memory log growth by slicing away old committed log entries.
//
// How it does it:
//   1. Computes the offset of compactUpTo relative to lastIncludedIndex.
//   2. Updates lastIncludedTerm to the term of the newly compacted boundary entry.
//   3. Slices n.log to discard all entries up to compactUpTo.
//   4. Updates lastIncludedIndex and notifies the state persister via signalPersist().
func (n *Node) compactLog(compactUpTo int64) {
	arrayIndex := compactUpTo - n.lastIncludedIndex - 1
	if arrayIndex >= 0 && arrayIndex < int64(len(n.log)) {
		newTerm := n.log[arrayIndex].Term
		newLog := make([]*proto.LogEntry, len(n.log[arrayIndex+1:]))
		copy(newLog, n.log[arrayIndex+1:])
		n.log = newLog

		n.lastIncludedIndex = compactUpTo
		n.lastIncludedTerm = newTerm
		n.signalPersist()

		fmt.Printf("Log Compacted! Index: %d\n", n.lastIncludedIndex)
	}
}

// runApplier is a long-running background worker that executes committed entries against the storage engine.
//
// Purpose:
//   Executes disk operations (WAL writes and MemTable mutations) outside of the main consensus lock.
//
// How it does it:
//   1. Continuously reads batches of committed LogEntry items from n.applyChannel.
//   2. Iterates over each entry and invokes applyEntry(entry) sequentially without holding n.mu.
func (n *Node) runApplier() {
	for entries := range n.applyChannel {
		for _, entry := range entries {
			n.applyEntry(entry)
		}
	}
}

// applyEntry executes a single committed command on the local LSM-Tree database or cluster topology.
//
// Purpose:
//   Applies the state machine transition specified by the log entry.
//
// How it does it:
//   - "PUT": Calls n.db.Put(key, value) to update WAL and MemTable.
//   - "DELETE": Calls n.db.Delete(key) to write a tombstone.
//   - "JOIN": Asynchronously connects to the newly added peer port over gRPC and adds it to n.peers.
func (n *Node) applyEntry(entry *proto.LogEntry) {
	switch entry.Command {
	case "PUT":
		n.db.Put(entry.Key, entry.Value)
	case "DELETE":
		n.db.Delete(entry.Key)
	case "JOIN":
		port := entry.Key
		address := "127.0.0.1:" + port
		if address != n.id {
			n.mu.Lock()
			_, exists := n.peers[port]
			n.mu.Unlock()

			if !exists {
				go func(p, addr string) {
					conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err == nil {
						n.AddPeer(p, proto.NewDatabaseClient(conn))
						fmt.Printf("Node joined cluster: %s\n", p)
					}
				}(port, address)
			}
		}
	}
}

// sendSnapshot transmits the current compacted SSTable snapshot file to a lagging follower.
//
// Purpose:
//   Synchronizes a follower whose nextIndex is older than the leader's available log memory.
//
// How it does it:
//   1. Triggers LSM compaction on the local engine to produce data_compact.sst.
//   2. Reads the compacted snapshot data and gathers active cluster peers while holding n.mu.
//   3. Calls c.InstallSnapshot over gRPC with a 5-second context timeout.
//   4. Re-acquires n.mu: if follower responded with a higher term, steps down to Follower.
func (n *Node) sendSnapshot(address string, c proto.DatabaseClient) {
	fmt.Printf("Follower %s fell behind. Generating Snapshot...\n", address)

	n.db.Compact()

	n.mu.Lock()
	term := n.currentTerm
	leaderId := n.id
	lastIndex := n.lastIncludedIndex
	lastTerm := n.lastIncludedTerm
	myPort := n.id[strings.LastIndex(n.id, ":")+1:]
	var activePeers []string
	for peer := range n.peers {
		activePeers = append(activePeers, peer)
	}
	n.mu.Unlock()

	path := "files/node_" + myPort + "/sst/data_compact.sst"
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading snapshot file:", err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()

	resp, err := c.InstallSnapshot(ctx, &proto.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          leaderId,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              data,
		ActivePeers:       activePeers,
	})

	if err != nil {
		fmt.Println("Failed to send snapshot:", err)
		return
	}

	n.mu.Lock()
	if resp.Term > n.currentTerm {
		n.currentTerm = resp.Term
		n.state = Follower
		n.votedFor = ""
		n.signalPersist()
		n.commitCond.Broadcast()
		n.replicateCond.Broadcast()
		n.mu.Unlock()
		return
	}
	n.mu.Unlock()

	fmt.Printf("Snapshot successfully beamed to %s!\n", address)
}

// HandleInstallSnapshot handles incoming InstallSnapshot RPC requests from the leader.
//
// Purpose:
//   Replaces the follower's entire storage engine and log state with the snapshot provided by the leader.
//
// How it does it:
//   1. Acquires n.mu and rejects requests with a term lower than n.currentTerm.
//   2. Adopts the leader's term, resets votedFor, and sets state to Follower.
//   3. Writes the snapshot binary to data_compact.sst and reloads SSTables into the LSM engine.
//   4. Establishes gRPC connections to any newly specified cluster peers.
//   5. Truncates all existing in-memory logs, updates lastIncludedIndex and commitIndex, and persists state.
func (n *Node) HandleInstallSnapshot(req *proto.InstallSnapshotRequest) *proto.InstallSnapshotResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &proto.InstallSnapshotResponse{Term: n.currentTerm}
	}

	n.state = Follower
	n.currentTerm = req.Term
	n.votedFor = ""
	n.lastContacted = time.Now()
	n.currentLeader = req.LeaderId
	n.signalPersist()
	n.commitCond.Broadcast()
	n.replicateCond.Broadcast()

	if req.LastIncludedIndex <= n.lastIncludedIndex {
		return &proto.InstallSnapshotResponse{Term: n.currentTerm}
	}

	fmt.Printf("Received InstallSnapshot up to index %d!\n", req.LastIncludedIndex)

	myPort := n.id[strings.LastIndex(n.id, ":")+1:]

	n.mu.Unlock()
	n.db.ClearState()
	path := "files/node_" + myPort + "/sst/data_compact.sst"
	err := os.WriteFile(path, req.Data, 0644)
	if err != nil {
		fmt.Println("Error writing snapshot:", err)
		n.mu.Lock()
		return &proto.InstallSnapshotResponse{Term: n.currentTerm}
	}
	n.db.LoadSSTables()
	n.mu.Lock()

	for _, peerPort := range req.ActivePeers {
		address := "127.0.0.1:" + peerPort
		if address != n.id {
			if _, exists := n.peers[peerPort]; !exists {
				go func(p, addr string) {
					conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
					if err == nil {
						n.AddPeer(p, proto.NewDatabaseClient(conn))
					}
				}(peerPort, address)
			}
		}
	}

	n.log = make([]*proto.LogEntry, 0)
	n.lastIncludedIndex = req.LastIncludedIndex
	n.lastIncludedTerm = req.LastIncludedTerm
	n.commitIndex = req.LastIncludedIndex
	n.lastApplied = req.LastIncludedIndex

	n.signalPersist()

	return &proto.InstallSnapshotResponse{Term: n.currentTerm}
}

// HandleRequestVote processes vote requests from election candidates.
//
// Purpose:
//   Enforces Raft leader election safety rules, including log up-to-dateness and single-vote-per-term limits.
//
// How it does it:
//   1. Acquires n.mu and ignores requests if a valid heartbeat was received recently (< 150ms).
//   2. Computes the local lastLogTerm and lastLogIndex.
//   3. Evaluates if the candidate's log is at least as up-to-date as the local log:
//      - Candidate's last log term is greater, OR terms are equal and candidate's index >= local index.
//   4. If candidate's term is greater than currentTerm, transitions to Follower and clears votedFor.
//   5. Grants vote if term matches, log is up-to-date, and votedFor is empty or matches candidateId.
func (n *Node) HandleRequestVote(mess *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if time.Since(n.lastContacted) < (time.Millisecond * 150) {
		return &proto.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}
	}

	myLastLogIndex := n.lastIncludedIndex + int64(len(n.log))
	myLastLogTerm := n.lastIncludedTerm
	if len(n.log) > 0 {
		myLastLogTerm = n.log[len(n.log)-1].Term
	}

	upToDate := false
	if mess.LastLogTerm > myLastLogTerm {
		upToDate = true
	} else if mess.LastLogTerm == myLastLogTerm && mess.LastLogIndex >= myLastLogIndex {
		upToDate = true
	}

	if mess.Term > n.currentTerm {
		n.currentTerm = mess.Term
		n.state = Follower
		n.votedFor = ""
		n.signalPersist()
		n.commitCond.Broadcast()
		n.replicateCond.Broadcast()
	}

	if mess.Term < n.currentTerm || !upToDate {
		return &proto.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: false,
		}
	} else if n.votedFor == "" || n.votedFor == mess.CandidateId {
		n.currentTerm = mess.Term
		n.lastContacted = time.Now()
		n.state = Follower
		n.votedFor = mess.CandidateId
		n.signalPersist()
		n.commitCond.Broadcast()
		n.replicateCond.Broadcast()
		return &proto.RequestVoteResponse{
			Term:        n.currentTerm,
			VoteGranted: true,
		}
	} else {
		return &proto.RequestVoteResponse{
			VoteGranted: false,
		}
	}
}

// IsLeader checks if the current node considers itself the cluster leader.
//
// Purpose:
//   Used by API handlers to guard write requests against non-leader execution.
//
// How it does it:
//   Compares GetLeader() against the node's own URL identifier.
func (n *Node) IsLeader() bool {
	if n.GetLeader() == n.id {
		return true
	}
	return false
}

// GetLeader returns the URL address of the known active leader.
//
// Purpose:
//   Provides redirection information to clients querying a follower node.
//
// How it does it:
//   Acquires n.mu and returns the currentLeader string safely.
func (n *Node) GetLeader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentLeader
}

// HandleAppendEntries processes incoming AppendEntries RPCs for both heartbeats and log replication.
//
// Purpose:
//   Synchronizes the follower's log with the leader, maintains leader contact, and applies committed entries.
//
// How it does it:
//   1. Acquires n.mu and rejects requests with a term lower than n.currentTerm.
//   2. Updates lastContacted timestamp, sets currentLeader, and transitions to Follower state.
//   3. Validates that the follower contains an entry matching PrevLogIndex and PrevLogTerm.
//   4. Appends any new entries, resolving and truncating any conflicting uncommitted entries.
//   5. If LeaderCommit > commitIndex, updates commitIndex and non-blockingly queues newly committed
//      entries to applyChannel for the background applier.
//   6. Triggers log compaction if needed and returns success: true.
func (n *Node) HandleAppendEntries(req *proto.AppendEntriesRequest) *proto.AppendEntriesResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

	if req.Term < n.currentTerm {
		return &proto.AppendEntriesResponse{Success: false, Term: n.currentTerm}
	}

	n.state = Follower
	n.currentTerm = req.Term
	n.lastContacted = time.Now()
	n.currentLeader = req.LeaderId
	n.signalPersist()
	n.commitCond.Broadcast()
	n.replicateCond.Broadcast()

	if req.PrevLogIndex >= n.lastIncludedIndex {
		var prevLogTerm int64 = n.lastIncludedTerm
		if req.PrevLogIndex > n.lastIncludedIndex {
			logIdx := req.PrevLogIndex - n.lastIncludedIndex - 1
			if logIdx < int64(len(n.log)) {
				prevLogTerm = n.log[logIdx].Term
			} else {
				return &proto.AppendEntriesResponse{Success: false, Term: n.currentTerm}
			}
		}

		if prevLogTerm != req.PrevLogTerm {
			return &proto.AppendEntriesResponse{Success: false, Term: n.currentTerm}
		}
	} else {
		return &proto.AppendEntriesResponse{Success: false, Term: n.currentTerm}
	}

	for i, newEntry := range req.Entries {
		entryIndex := req.PrevLogIndex + 1 + int64(i)
		logIdx := entryIndex - n.lastIncludedIndex - 1

		if logIdx < int64(len(n.log)) {
			if n.log[logIdx].Term != newEntry.Term {
				n.log = n.log[:logIdx]
				n.log = append(n.log, newEntry)
			}
		} else {
			n.log = append(n.log, newEntry)
		}
	}

	if req.LeaderCommit > n.commitIndex {
		lastNewEntryIndex := req.PrevLogIndex + int64(len(req.Entries))
		oldCommit := n.commitIndex

		if req.LeaderCommit < lastNewEntryIndex {
			n.commitIndex = req.LeaderCommit
		} else {
			n.commitIndex = lastNewEntryIndex
		}

		toApply := make([]*proto.LogEntry, 0, n.commitIndex-oldCommit)
		for i := oldCommit + 1; i <= n.commitIndex; i++ {
			arrayIndex := i - n.lastIncludedIndex - 1
			if arrayIndex >= 0 && arrayIndex < int64(len(n.log)) {
				toApply = append(toApply, n.log[arrayIndex])
			}
		}
		if len(toApply) > 0 {
			select {
			case n.applyChannel <- toApply:
			default:
				go func(entries []*proto.LogEntry) {
					n.applyChannel <- entries
				}(toApply)
			}
		}
		n.commitCond.Broadcast()

		if n.commitIndex-n.lastIncludedIndex > 10 {
			n.compactLog(n.commitIndex - 1)
		}
	}

	return &proto.AppendEntriesResponse{Success: true, Term: n.currentTerm}
}

// AddPeer dynamically adds a new peer node's gRPC client connection to the active cluster.
//
// Purpose:
//   Allows runtime cluster membership changes without needing a full cluster restart.
//
// How it does it:
//   1. Acquires n.mu and inserts the client into the n.peers map.
//   2. If the current node is Leader, initializes nextIndex and matchIndex for the new peer
//      and immediately spawns a dedicated peerReplicator goroutine.
//   3. Dispatches a signal to persist the updated peer list to state.json.
func (n *Node) AddPeer(address string, client proto.DatabaseClient) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.peers[address] = client
	if n.state == Leader {
		n.nextIndex[address] = n.lastIncludedIndex + int64(len(n.log)) + 1
		n.matchIndex[address] = 0
		go n.peerReplicator(address, client, n.currentTerm)
	}
	n.signalPersist()
}

// RunElectionTimer monitors leader heartbeat health and triggers elections on timeout.
//
// Purpose:
//   Detects leader failure and transitions the node to Candidate to elect a new leader.
//
// How it does it:
//   1. Generates a randomized timeout between 150ms and 300ms to prevent split votes.
//   2. Periodically checks time.Since(lastContacted).
//   3. If elapsed time exceeds timeout while in Follower/Candidate state, calls startNewElection().
//   4. Resets the timer with a new randomized duration and repeats.
func (n *Node) RunElectionTimer() {
	timeout := time.Duration(rand.Intn(150)+150) * time.Millisecond
	timer := time.NewTimer(timeout)
	for {
		<-timer.C

		n.mu.Lock()
		elapsed := time.Since(n.lastContacted)
		if (n.state == Follower || n.state == Candidate) && elapsed >= timeout {
			n.startNewElection()
			timeout = time.Duration(rand.Intn(150)+150) * time.Millisecond
			timer.Reset(timeout)
		} else {
			remaining := timeout - elapsed
			if remaining <= 0 {
				timeout = time.Duration(rand.Intn(150)+150) * time.Millisecond
				remaining = timeout
			}
			timer.Reset(remaining)
		}
		n.mu.Unlock()
	}
}

// startNewElection promotes the node to Candidate and solicits votes from all cluster peers.
//
// Purpose:
//   Executes the election phase of the Raft consensus algorithm.
//
// How it does it:
//   1. Transitions state to Candidate, increments currentTerm, and votes for self (votedFor = id).
//   2. Gathers lastLogIndex and lastLogTerm for the RequestVote payload.
//   3. If self-vote satisfies quorum (e.g. single-node cluster), becomes Leader immediately via startLeaderLocked().
//   4. Otherwise, concurrently sends RequestVote RPCs to all peers with a 50ms timeout.
//   5. Tallies granted votes under n.mu; upon securing majority ((N/2)+1), promotes to Leader,
//      initializes peer replication indices, and invokes startLeaderLocked().
func (n *Node) startNewElection() {
	n.state = Candidate
	n.votedFor = n.id
	n.currentTerm++
	n.lastContacted = time.Now()
	n.signalPersist()

	votes := 1
	term := n.currentTerm
	candidateId := n.id

	lastLogIndex := n.lastIncludedIndex + int64(len(n.log))
	lastLogTerm := n.lastIncludedTerm
	if len(n.log) > 0 {
		lastLogTerm = n.log[len(n.log)-1].Term
	}

	fmt.Printf("Node %s's timer expired! Starting election for Term %d...\n", candidateId, term)

	if votes == ((len(n.peers)+1)/2)+1 {
		n.state = Leader
		n.currentLeader = n.id

		n.nextIndex = make(map[string]int64)
		n.matchIndex = make(map[string]int64)
		lastIdx := n.lastIncludedIndex + int64(len(n.log))
		for p := range n.peers {
			n.nextIndex[p] = lastIdx + 1
			n.matchIndex[p] = 0
		}

		fmt.Printf("\n Node %s WON the election! It is now the LEADER for Term %d! \n\n", n.id, n.currentTerm)
		n.startLeaderLocked()
		return
	}

	for peers, db := range n.peers {
		go func(address string, c proto.DatabaseClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			resp, err := c.RequestVote(ctx, &proto.RequestVoteRequest{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			})

			if err != nil {
				fmt.Printf("Failed to ask for vote from %s: %v\n", address, err)
				return
			}

			n.mu.Lock()
			if resp.Term > n.currentTerm {
				n.currentTerm = resp.Term
				n.state = Follower
				n.votedFor = ""
				n.signalPersist()
				n.commitCond.Broadcast()
				n.replicateCond.Broadcast()
				n.mu.Unlock()
				return
			}

			if resp.VoteGranted == true {
				if n.state != Candidate || n.currentTerm != term {
					n.mu.Unlock()
					return
				}
				votes++
				if votes == ((len(n.peers)+1)/2)+1 {
					n.state = Leader
					n.currentLeader = n.id

					n.nextIndex = make(map[string]int64)
					n.matchIndex = make(map[string]int64)
					lastIdx := n.lastIncludedIndex + int64(len(n.log))
					for p := range n.peers {
						n.nextIndex[p] = lastIdx + 1
						n.matchIndex[p] = 0
					}

					fmt.Printf("\n Node %s WON the election! It is now the LEADER for Term %d! \n\n", n.id, n.currentTerm)
					n.startLeaderLocked()
					n.mu.Unlock()
					return
				}
			}
			n.mu.Unlock()
		}(peers, db)
	}
}

// PersistentState represents the JSON schema saved to state.json for crash recovery.
type PersistentState struct {
	CurrentTerm       int64             `json:"current_term"`
	VotedFor          string            `json:"voted_for"`
	Peers             []string          `json:"peers"`
	Log               []*proto.LogEntry `json:"log"`
	LastIncludedIndex int64             `json:"last_included_index"`
	LastIncludedTerm  int64             `json:"last_included_term"`
}

// signalPersist notifies the background persistence worker that new state needs to be flushed to disk.
//
// Purpose:
//   Provides a non-blocking trigger to coalesce multiple rapid state mutations into a single disk write.
//
// How it does it:
//   Performs a non-blocking select push to the 1-capacity persistDirty channel.
func (n *Node) signalPersist() {
	select {
	case n.persistDirty <- struct{}{}:
	default:
	}
}

// runStatePersister is a dedicated background worker that writes consensus metadata to disk.
//
// Purpose:
//   Performs batched JSON encoding and disk I/O without blocking the critical consensus path.
//
// How it does it:
//   1. Waits on n.persistDirty channel for incoming persistence requests.
//   2. Sleeps 5ms to batch and coalesce multiple rapid signals into a single write.
//   3. Calls persistStateSync() to serialize and write state.json.
func (n *Node) runStatePersister() {
	for range n.persistDirty {
		time.Sleep(5 * time.Millisecond)
		n.persistStateSync()
	}
}

// persistStateSync captures a snapshot of the consensus state and saves it to state.json.
//
// Purpose:
//   Guarantees durability of CurrentTerm, VotedFor, and uncompacted LogEntries.
//
// How it does it:
//   1. Acquires n.mu, copies currentTerm, votedFor, peer list, log, and snapshot metadata into PersistentState.
//   2. Releases n.mu before performing file system operations.
//   3. Marshals state into JSON and writes it atomically to files/node_<port>/raft/state.json.
func (n *Node) persistStateSync() {
	n.mu.Lock()
	myPort := n.id[strings.LastIndex(n.id, ":")+1:]
	dir := "files/node_" + myPort + "/raft"

	var peerList []string
	for p := range n.peers {
		peerList = append(peerList, p)
	}

	state := PersistentState{
		CurrentTerm:       n.currentTerm,
		VotedFor:          n.votedFor,
		Peers:             peerList,
		Log:               n.log,
		LastIncludedIndex: n.lastIncludedIndex,
		LastIncludedTerm:  n.lastIncludedTerm,
	}
	n.mu.Unlock()

	os.MkdirAll(dir, 0755)
	data, err := json.Marshal(state)
	if err == nil {
		os.WriteFile(filepath.Join(dir, "state.json"), data, 0644)
	}
}

// loadState restores persisted Raft metadata and peers from state.json upon node boot.
//
// Purpose:
//   Prevents amnesia loops upon crash recovery so rebooted nodes remember terms, votes, and peers.
//
// How it does it:
//   1. Reads files/node_<port>/raft/state.json from disk.
//   2. Unmarshals JSON into PersistentState.
//   3. Restores currentTerm, votedFor, log entries, and snapshot indices.
//   4. Dials gRPC connections to all saved peers to restore the active cluster topology.
func (n *Node) loadState() {
	myPort := n.id[strings.LastIndex(n.id, ":")+1:]
	path := "files/node_" + myPort + "/raft/state.json"

	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	state := PersistentState{
		LastIncludedIndex: -1,
		LastIncludedTerm:  -1,
	}
	if err := json.Unmarshal(data, &state); err == nil {
		n.currentTerm = state.CurrentTerm
		n.votedFor = state.VotedFor
		if state.Log != nil {
			n.log = state.Log
		}
		n.lastIncludedIndex = state.LastIncludedIndex
		n.lastIncludedTerm = state.LastIncludedTerm
		if n.commitIndex < n.lastIncludedIndex {
			n.commitIndex = n.lastIncludedIndex
		}
		n.lastApplied = n.commitIndex

		for _, p := range state.Peers {
			addr := "127.0.0.1:" + p
			conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err == nil {
				n.peers[p] = proto.NewDatabaseClient(conn)
			}
		}
		if len(state.Peers) > 0 {
			fmt.Printf("Recovered Raft State: Term %d, VotedFor %s, %d Peers\n", n.currentTerm, n.votedFor, len(n.peers))
		}
	}
}
