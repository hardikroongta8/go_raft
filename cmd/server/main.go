package main

import (
	"flag"
	"fmt"
	"github.com/hardikroongta8/go_raft/internal/raft"
	"github.com/hardikroongta8/go_raft/internal/server"
	"sync"
)

func main() {
	id := flag.Int("id", 1, "node id")
	flag.Parse()
	peers := make(map[raft.NodeID]string)

	peers[1] = "127.0.0.1:8081"
	peers[2] = "127.0.0.1:8082"
	peers[3] = "127.0.0.1:8083"

	port := 8080 + *id
	var wg sync.WaitGroup
	srv := server.NewCacheServer(port, raft.NodeID(*id), peers)
	fmt.Printf("[Node %d] Listening to server on port %d...\n", *id, port)
	wg.Add(1)

	go srv.Start(&wg)
	select {}
	//ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT)
	//defer stop()
	//<-ctx.Done()
	//
	//srv.Quit()
	//wg.Wait()
	//os.Exit(0)
}
