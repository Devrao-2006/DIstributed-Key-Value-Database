package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"DDB/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	target := "127.0.0.1:50051"
	if len(os.Args) > 1 {
		target = os.Args[1]
		if !strings.Contains(target, ":") {
			target = "127.0.0.1:" + target
		}
	}

	fmt.Printf("Dialing cluster node at %s...\n", target)
	conn, err := grpc.Dial(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Did not connect: %v", err)
	}
	defer conn.Close()
	client := proto.NewDatabaseClient(conn)

	fmt.Println("Welcome to DDB CLI! Commands: PUT <key> <value> | GET <key> | DELETE <key> | exit")
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("ddb> ")
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "exit" {
			fmt.Println("Goodbye!")
			break
		}

		parts := strings.Split(line, " ")
		if len(parts) == 0 || parts[0] == "" {
			continue
		}

		cmd := strings.ToUpper(parts[0])
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)

		switch cmd {
		case "PUT":
			if len(parts) < 3 {
				fmt.Println("Usage: PUT <key> <value>")
				cancel()
				continue
			}
			res, err := client.Put(ctx, &proto.PutRequest{Key: parts[1], Value: parts[2]})
			if err != nil {
				fmt.Println("Error:", err)
			} else if res.Success {
				fmt.Println("OK")
			} else {
				fmt.Println("Failed:", res.ErrorMessage)
			}

		case "GET":
			if len(parts) < 2 {
				fmt.Println("Usage: GET <key>")
				cancel()
				continue
			}
			res, err := client.Get(ctx, &proto.GetRequest{Key: parts[1]})
			if err != nil {
				fmt.Println("Error:", err)
			} else if res.Success {
				fmt.Printf("\"%s\"\n", res.Value)
			} else {
				fmt.Println("Failed:", res.ErrorMessage)
			}

		case "DELETE":
			if len(parts) < 2 {
				fmt.Println("Usage: DELETE <key>")
				cancel()
				continue
			}
			res, err := client.Delete(ctx, &proto.DeleteRequest{Key: parts[1]})
			if err != nil {
				fmt.Println("Error:", err)
			} else if res.Success {
				fmt.Println("OK")
			} else {
				fmt.Println("Failed:", res.ErrorMessage)
			}

		default:
			fmt.Println("Unknown command:", cmd)
		}

		cancel()
	}
}
