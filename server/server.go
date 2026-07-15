package server

import (
	"DDB/engine"
	"DDB/proto"
	"DDB/raft"
	"context"
)

type Server struct {
	proto.UnimplementedDatabaseServer
	db       *engine.MemoryEngine
	raftNode *raft.Node
}

func NewServer(db *engine.MemoryEngine, raft *raft.Node) *Server {
	return &Server{
		db:       db,
		raftNode: raft,
	}
}

func (s *Server) Put(ctx context.Context, req *proto.PutRequest) (*proto.PutResponse, error) {
	if check := s.raftNode.IsLeader(); check == false {
		return &proto.PutResponse{Success: false, ErrorMessage: "Not the Leader. Try " + s.raftNode.GetLeader()}, nil
	}
	err := s.raftNode.Propose("PUT", req.Key, req.Value)
	if err != nil {
		return &proto.PutResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &proto.PutResponse{Success: true}, nil
}

func (s *Server) Get(ctx context.Context, req *proto.GetRequest) (*proto.GetResponse, error) {
	val, err := s.db.Get(req.Key)
	if err != nil {
		return &proto.GetResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &proto.GetResponse{Success: true, Value: val}, nil
}

func (s *Server) Delete(ctx context.Context, req *proto.DeleteRequest) (*proto.DeleteResponse, error) {
	if check := s.raftNode.IsLeader(); check == false {
		return &proto.DeleteResponse{Success: false, ErrorMessage: "Not the Leader. Try " + s.raftNode.GetLeader()}, nil
	}
	err := s.raftNode.Propose("DELETE", req.Key, "")
	if err != nil {
		return &proto.DeleteResponse{Success: false, ErrorMessage: err.Error()}, nil
	}
	return &proto.DeleteResponse{Success: true}, nil
}

func (s *Server) RequestVote(ctx context.Context, req *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	return s.raftNode.HandleRequestVote(req), nil
}

func (s *Server) AppendEntries(ctx context.Context, req *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	return s.raftNode.HandleAppendEntries(req), nil
}

func (s *Server) InstallSnapshot(ctx context.Context, req *proto.InstallSnapshotRequest) (*proto.InstallSnapshotResponse, error) {
	return s.raftNode.HandleInstallSnapshot(req), nil
}

func (s *Server) JoinCluster(ctx context.Context, req *proto.JoinClusterRequest) (*proto.JoinClusterResponse, error) {
	if !s.raftNode.IsLeader() {
		return &proto.JoinClusterResponse{Success: false}, nil
	}
	
	err := s.raftNode.Propose("JOIN", req.Port, "")
	if err != nil {
		return &proto.JoinClusterResponse{Success: false}, nil
	}
	
	return &proto.JoinClusterResponse{Success: true}, nil
}
