package raft

import (
	"context"
	"fmt"
	"github.com/hardikroongta8/go_raft/pkg/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"net"
	"sync"
)

type Transport struct {
	srv     *grpc.Server             // listens to RPCs from its peers
	clients map[NodeID]pb.RaftClient // maps peerID to raft client
	peers   map[NodeID]string        // maps peerID to address
	id      NodeID                   // nodeID
	mu      sync.Mutex
}

func NewTransport(id NodeID, peers map[NodeID]string) *Transport {
	return &Transport{
		clients: make(map[NodeID]pb.RaftClient),
		peers:   peers,
		id:      id,
	}
}

func (t *Transport) Start(srv *Node, ln net.Listener) {
	t.srv = grpc.NewServer()
	pb.RegisterRaftServer(t.srv, srv)

	fmt.Printf("[Node %d] Starting gRPC Server...\n", t.id)
	if err := t.srv.Serve(ln); err != nil {
		fmt.Printf("[Node %d] Error starting gRPC Server: %s\n", t.id, err.Error())
	}
}

func (t *Transport) Stop() {
	if t.srv != nil {
		t.srv.GracefulStop()
	}
}

func (t *Transport) GetClient(peerID NodeID) (pb.RaftClient, error) {
	if client, ok := t.clients[peerID]; ok {
		return client, nil
	}
	addr, exists := t.peers[peerID]
	if !exists {
		return nil, fmt.Errorf("invalid peerID: %d", peerID)
	}
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	t.clients[peerID] = pb.NewRaftClient(conn)
	return t.clients[peerID], nil
}

func (t *Transport) sendVoteRequest(ctx context.Context, pID NodeID, args *pb.VoteRequestArgs) (*pb.VoteResponse, error) {
	client, err := t.GetClient(pID)
	if err != nil {
		return nil, err
	}
	return client.VoteRequest(ctx, args)
}

func (t *Transport) sendLogRequest(ctx context.Context, pID NodeID, args *pb.LogRequestArgs) (*pb.LogResponse, error) {
	client, err := t.GetClient(pID)
	if err != nil {
		return nil, err
	}
	return client.LogRequest(ctx, args)
}
