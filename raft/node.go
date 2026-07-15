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
	lastIncludedTerm  int64
	lastIncludedIndex int64
	snapshotting      map[string]bool
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
		snapshotting:      make(map[string]bool),
	}
	node.loadState()
	return node
}

func (n *Node) Propose(command, key, value string) error {
	n.mu.Lock()
	var logEntry = proto.LogEntry{Term: n.currentTerm, Command: command, Key: key, Value: value}
	n.log = append(n.log, &logEntry)
	targetCommit := n.lastIncludedIndex + 1 + int64(len(n.log))
	n.mu.Unlock()

	for {
		n.mu.Lock()
		if n.commitIndex >= targetCommit {
			n.mu.Unlock()
			return nil
		}
		if n.state != Leader {
			n.mu.Unlock()
			return fmt.Errorf("Lost leadership before commit")
		}
		n.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func (n *Node) startHeartbeat() {
	for {
		n.mu.Lock()
		if n.state != Leader {
			n.mu.Unlock()
			return
		}
		term := n.currentTerm
		Leaderid := n.id

		logs := make([]*proto.LogEntry, len(n.log))
		copy(logs, n.log)
		commitIndex := n.commitIndex
		
		peersCopy := make(map[string]proto.DatabaseClient)
		for k, v := range n.peers {
			peersCopy[k] = v
		}
		n.mu.Unlock()

		var successes int = 1
		peerCount := len(peersCopy)
		voteChan := make(chan bool, peerCount)

		for peer, db := range peersCopy {
			go func(address string, c proto.DatabaseClient) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*50)
				defer cancel()

				resp, err := c.AppendEntries(ctx, &proto.AppendEntriesRequest{
					Term:         term,
					LeaderId:     Leaderid,
					Entries:      logs,
					LeaderCommit: commitIndex,
					PrevLogIndex: n.lastIncludedIndex,
					PrevLogTerm:  n.lastIncludedTerm,
				})

				if err == nil {
					if resp.NeedsSnapshot {
						n.mu.Lock()
						if !n.snapshotting[address] {
							n.snapshotting[address] = true
							go func(addr string, client proto.DatabaseClient) {
								n.sendSnapshot(addr, client)
								n.mu.Lock()
								n.snapshotting[addr] = false
								n.mu.Unlock()
							}(address, c)
						}
						n.mu.Unlock()
						voteChan <- false
					} else if resp.Success {
						voteChan <- true
					} else {
						voteChan <- false
					}
				} else {
					voteChan <- false
				}
			}(peer, db)
		}

		for i := 0; i < peerCount; i++ {
			if <-voteChan {
				successes++
			}
			if successes > (peerCount+1)/2 {
				break
			}
		}

		n.mu.Lock()
		if successes > (peerCount+1)/2 {
			targetCommitIndex := n.lastIncludedIndex + 1 + int64(len(logs))

			if targetCommitIndex > n.commitIndex {
				for i := n.commitIndex; i < targetCommitIndex; i++ {
					arrayIndex := i - n.lastIncludedIndex - 1
					if arrayIndex >= 0 && arrayIndex < int64(len(n.log)) {
						entry := n.log[arrayIndex]
						switch entry.Command {
						case "PUT":
							n.db.Put(entry.Key, entry.Value)
						case "DELETE":
							n.db.Delete(entry.Key)
						case "JOIN":
							port := entry.Key
							address := "127.0.0.1:" + port
							if address != n.id {
								if _, exists := n.peers[port]; !exists {
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
				}
				n.commitIndex = targetCommitIndex
				if n.commitIndex-n.lastIncludedIndex > 10 {
					compactUpTo := n.commitIndex - 1
					arrayIndex := compactUpTo - n.lastIncludedIndex - 1

					newTerm := n.log[arrayIndex].Term
					newLog := make([]*proto.LogEntry, len(n.log[arrayIndex+1:]))
					copy(newLog, n.log[arrayIndex+1:])
					n.log = newLog

					n.lastIncludedIndex = compactUpTo
					n.lastIncludedTerm = newTerm

					fmt.Printf("Log Compacted! Index: %d\n", n.lastIncludedIndex)
				}

			}
		}
		n.mu.Unlock()

		time.Sleep(time.Millisecond * 50)
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
	n.mu.Unlock()

	path := "files/node_" + myPort + "/sst/data_compact.sst"
	data, err := os.ReadFile(path)
	if err != nil {
		fmt.Println("Error reading snapshot file:", err)
		return
	}

	var activePeers []string
	for peer := range n.peers {
		activePeers = append(activePeers, peer)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
	defer cancel()
	
	_, err = c.InstallSnapshot(ctx, &proto.InstallSnapshotRequest{
		Term:              term,
		LeaderId:          leaderId,
		LastIncludedIndex: lastIndex,
		LastIncludedTerm:  lastTerm,
		Data:              data,
		ActivePeers:       activePeers,
	})
	
	if err != nil {
		fmt.Println("Failed to send snapshot:", err)
	} else {
		fmt.Printf("Snapshot successfully beamed to %s!\n", address)
	}
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

	if req.LastIncludedIndex <= n.lastIncludedIndex {
		return &proto.InstallSnapshotResponse{Term: n.currentTerm}
	}

	fmt.Printf("Received InstallSnapshot up to index %d!\n", req.LastIncludedIndex)

	n.db.ClearState()

	myPort := n.id[strings.LastIndex(n.id, ":")+1:] 
	path := "files/node_" + myPort + "/sst/data_compact.sst"
	err := os.WriteFile(path, req.Data, 0644)
	if err != nil {
		fmt.Println("Error writing snapshot:", err)
		return &proto.InstallSnapshotResponse{Term: n.currentTerm}
	}

	n.db.LoadSSTables()

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

	n.persistState()

	return &proto.InstallSnapshotResponse{Term: n.currentTerm}
}

func (n *Node) HandleRequestVote(mess *proto.RequestVoteRequest) *proto.RequestVoteResponse {
	n.mu.Lock()
	defer n.mu.Unlock()

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
		n.persistState()
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
		n.persistState()
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
		return &proto.AppendEntriesResponse{
			Success: false,
		}
	} else {
		if req.PrevLogIndex >= 0 && n.commitIndex < req.PrevLogIndex {
			n.lastContacted = time.Now()
			n.currentTerm = req.Term
			return &proto.AppendEntriesResponse{
				Success:       false,
				NeedsSnapshot: true,
			}
		}

		n.state = Follower
		n.currentTerm = req.Term
		n.lastContacted = time.Now()
		n.currentLeader = req.LeaderId
		n.lastIncludedIndex = req.PrevLogIndex
		n.lastIncludedTerm = req.PrevLogTerm
		n.log = req.Entries
		
		n.persistState()

		if req.LeaderCommit > n.commitIndex {
			for i := n.commitIndex; i < req.LeaderCommit; i++ {
				arrayIndex := i - req.PrevLogIndex - 1
				if arrayIndex >= 0 && arrayIndex < int64(len(req.Entries)) {
					entry := req.Entries[arrayIndex]
					switch entry.Command {
					case "PUT":
						n.db.Put(entry.Key, entry.Value)
					case "DELETE":
						n.db.Delete(entry.Key)
					case "JOIN":
						port := entry.Key
						address := "127.0.0.1:" + port
						if address != n.id {
							if _, exists := n.peers[port]; !exists {
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
			}
			n.commitIndex = req.LeaderCommit
		}

		return &proto.AppendEntriesResponse{
			Success: true,
		}
	}
}

func (n *Node) AddPeer(address string, client proto.DatabaseClient) {
	n.mu.Lock()
	n.peers[address] = client
	n.persistState()
	n.mu.Unlock()
}

func (n *Node) RunElectionTimer() {
	for {
		timer := rand.Intn(150) + 150
		time.Sleep(time.Duration(timer) * time.Millisecond)

		n.mu.Lock()
		if n.state == Follower || n.state == Candidate {
			if time.Since(n.lastContacted) > (time.Duration(timer) * time.Millisecond) {
				n.startNewElection()
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) startNewElection() {
	n.state = Candidate
	n.votedFor = n.id
	n.currentTerm++
	n.lastContacted = time.Now()
	n.persistState()

	votes := 1
	term := n.currentTerm
	candidateId := n.id
	
	lastLogIndex := n.lastIncludedIndex + int64(len(n.log))
	lastLogTerm := n.lastIncludedTerm
	if len(n.log) > 0 {
		lastLogTerm = n.log[len(n.log)-1].Term
	}

	fmt.Printf("Node %s's timer expired! Starting election for Term %d...\n", candidateId, term)

	if votes > (len(n.peers)+1)/2 {
		n.state = Leader
		n.currentLeader = n.id
		fmt.Printf("\n Node %s WON the election! It is now the LEADER for Term %d! \n\n", n.id, n.currentTerm)
		go n.startHeartbeat()
		return
	}

	for peers, db := range n.peers {
		go func(address string, c proto.DatabaseClient) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			resp, err := c.RequestVote(ctx, &proto.RequestVoteRequest{
				Term: term, 
				CandidateId: candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm: lastLogTerm,
			})

			if err != nil {
				fmt.Printf("Failed to ask for vote from %s: %v\n", address, err)
				return
			}

			if resp.VoteGranted == true {
				n.mu.Lock()
				defer n.mu.Unlock()
				if resp.Term > n.currentTerm {
					n.currentTerm = resp.Term
					n.state = Follower
					n.votedFor = ""
					n.persistState()
					return
				}
				if n.state != Candidate {
					return
				}
				votes++
				if votes > (len(n.peers)+1)/2 {
					n.state = Leader
					n.currentLeader = n.id
					fmt.Printf("\n Node %s WON the election! It is now the LEADER for Term %d! \n\n", n.id, n.currentTerm)
					go n.startHeartbeat()
				}
			}
		}(peers, db)
	}
}

type PersistentState struct {
	CurrentTerm int64    `json:"current_term"`
	VotedFor    string   `json:"voted_for"`
	Peers       []string `json:"peers"`
}

func (n *Node) persistState() {
	myPort := n.id[strings.LastIndex(n.id, ":")+1:]
	dir := "files/node_" + myPort + "/raft"
	os.MkdirAll(dir, 0755)

	var peerList []string
	for p := range n.peers {
		peerList = append(peerList, p)
	}

	state := PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
		Peers:       peerList,
	}

	data, _ := json.Marshal(state)
	os.WriteFile(filepath.Join(dir, "state.json"), data, 0644)
}

func (n *Node) loadState() {
	myPort := n.id[strings.LastIndex(n.id, ":")+1:]
	path := "files/node_" + myPort + "/raft/state.json"
	
	data, err := os.ReadFile(path)
	if err != nil {
		return
	}

	var state PersistentState
	if err := json.Unmarshal(data, &state); err == nil {
		n.currentTerm = state.CurrentTerm
		n.votedFor = state.VotedFor
		
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
