package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os/signal"
	"raft/internal/config"
	"raft/internal/raft"
	"syscall"
)

var (
	port          = flag.Int("port", 0, "port to listen on")
	bootstrapFile = flag.String("bf", "", "file to bootstrap raft cluster")
)

func main() {
	flag.Parse()
	if *port == 0 {
		log.Fatal("port must be specified")
	}
	if *bootstrapFile == "" {
		log.Fatal("file to bootstrap raft cluster must be specified")
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	c, err := config.Parse(*bootstrapFile)
	if err != nil {
		log.Fatal(fmt.Errorf("parse config: %w", err))
	}

	server := raft.NewServer(ctx, *port, c.ServersInCluster)
	server.Start()
}
