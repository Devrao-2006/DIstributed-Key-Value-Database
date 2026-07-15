package main

import (
	"DDB/engine"
	"DDB/memtable"
	"DDB/proto"
	"DDB/raft"
	"DDB/server"
	"DDB/wal"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"net"
	"os"
	"time"
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	ErrPathNotFound = errors.New("Path Not Found For WAL Initialization")
	ErrRecovery     = errors.New("Recovery Didnt Happen")
)

func main() {
	rand.Seed(time.Now().UnixNano())

	if len(os.Args) < 2 {
		log.Fatal("Please provide a port number! (e.g. 50051)")
	}
	myPort := os.Args[1]
	myAddress := "127.0.0.1:" + myPort
	
	nodeDir := "files/node_" + myPort
	err := os.MkdirAll(nodeDir + "/sst", 0755)
	if err != nil {
		log.Fatalf("Failed to create directory: %v", err)
	}

	w := wal.NewWalEngine()
	err = w.Open(nodeDir + "/wal.wal")
	if err != nil {
		fmt.Println(ErrPathNotFound, err)
		return
	}

	avl := memtable.NewAVLTree()
	db := engine.NewMemoryEngine(w, avl, nodeDir + "/sst")

	err = db.Recovery()
	if err != nil {
		fmt.Println("No existing WAL found or recovery failed (this is normal for a new node).")
	}

	raftNode := raft.NewNode(myAddress, db)
	go raftNode.RunElectionTimer()

	if len(os.Args) == 3 {
		joinPort := os.Args[2]
		conn, err := grpc.Dial("127.0.0.1:"+joinPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			client := proto.NewDatabaseClient(conn)
			raftNode.AddPeer(joinPort, client)

			ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
			defer cancel()
			
			resp, err := client.JoinCluster(ctx, &proto.JoinClusterRequest{Port: myPort})
			if err != nil || !resp.Success {
				log.Fatalf("Failed to join cluster via %s. Error/Response: %v", joinPort, err)
			}
			fmt.Printf("Successfully joined the cluster via %s!\n", joinPort)
		} else {
			log.Fatalf("Could not dial seed node %s: %v", joinPort, err)
		}
	} else {
		fmt.Println("Starting as a brand new cluster (Seed Node).")
	}

	lis, err := net.Listen("tcp", ":"+myPort)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	log.Printf("Database listening on port %s...\n", myPort)

	grpcServer := grpc.NewServer()
	proto.RegisterDatabaseServer(grpcServer, server.NewServer(db, raftNode))

	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}
