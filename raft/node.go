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

const (
	Follower  state = 1
	Candidate state = 2
	Leader    state = 3
)

type state uint8

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

func (n *Node) runApplier() {
	for entries := range n.applyChannel {
		for _, entry := range entries {
			n.applyEntry(entry)
		}
	}
}

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

func (n *Node) IsLeader() bool {
	if n.GetLeader() == n.id {
		return true
	}
	return false
}

func (n *Node) GetLeader() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentLeader
}

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

type PersistentState struct {
	CurrentTerm       int64             `json:"current_term"`
	VotedFor          string            `json:"voted_for"`
	Peers             []string          `json:"peers"`
	Log               []*proto.LogEntry `json:"log"`
	LastIncludedIndex int64             `json:"last_included_index"`
	LastIncludedTerm  int64             `json:"last_included_term"`
}

func (n *Node) signalPersist() {
	select {
	case n.persistDirty <- struct{}{}:
	default:
	}
}

func (n *Node) runStatePersister() {
	for range n.persistDirty {
		time.Sleep(5 * time.Millisecond)
		n.persistStateSync()
	}
}

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
