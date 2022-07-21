package main

import (
	blacklist "blacklist/protos"
	"flag"
	"fmt"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
)

var (
	port = flag.Int("port", 50051, "The server port")
)

func main() {
	flag.Parse()
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	server := grpc.NewServer(opts...)
	table := os.Getenv("BLACKLIST_TABLE")
	blacklist.RegisterBlacklistServer(server, &BlacklistServer{table: table, batchSize: 25})
	err = server.Serve(listener)
	if err != nil {
		return
	}
}