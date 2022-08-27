package main

import (
	"blacklist/apis"
	"blacklist/tools/protos"
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
	err := os.Setenv("AWS_SDK_LOAD_CONFIG", "true")
	if err != nil {
		log.Fatalf("failed to start: %v", err)
	}
	flag.Parse()
	listener, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", *port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	server := grpc.NewServer(opts...)
	table := os.Getenv("BLACKLIST_TABLE")
	blacklist.RegisterBlacklistServer(server, &apis.BlacklistServer{Table: table, BatchSize: 25})
	err = server.Serve(listener)
	if err != nil {
		return
	}
}
