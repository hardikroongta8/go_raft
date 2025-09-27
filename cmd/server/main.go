package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/hardikroongta8/go_raft/internal/raft"
	"github.com/hardikroongta8/go_raft/internal/server"
)

func main() {
	id := flag.Int("id", 1, "node id")
	flag.Parse()
	peers := make(map[raft.NodeID]string)

	peers[1] = "localhost:8081"
	peers[2] = "localhost:8082"
	peers[3] = "localhost:8083"

	port := 8080 + *id
	var wg sync.WaitGroup
	srv := server.NewCacheServer(port, raft.NodeID(*id), peers)
	fmt.Printf("[Node %d] Listening to server on port %d...\n", *id, port)
	wg.Add(1)

	go srv.Start(&wg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
	defer stop()
	<-ctx.Done()

	srv.Quit()
	wg.Wait()
	os.Exit(0)
}
