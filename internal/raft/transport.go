package raft

import (
	"context"
	"fmt"
	"net"

	pb2 "github.com/hardikroongta8/go_raft/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Transport struct {
	srv     *grpc.Server              // listens to RPCs from its peers
	clients map[NodeID]pb2.RaftClient // maps peerID to raft client
	peers   map[NodeID]string         // maps peerID to address
	id      NodeID                    // nodeID
}

func NewTransport(id NodeID, peers map[NodeID]string) *Transport {
	return &Transport{
		clients: make(map[NodeID]pb2.RaftClient),
		peers:   peers,
		id:      id,
	}
}

func (t *Transport) Start(srv *Node, ln net.Listener) {
	t.srv = grpc.NewServer()
	pb2.RegisterRaftServer(t.srv, srv)

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

func (t *Transport) GetClient(peerID NodeID) (pb2.RaftClient, error) {
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
	t.clients[peerID] = pb2.NewRaftClient(conn)
	return t.clients[peerID], nil
}

func (t *Transport) sendVoteRequest(ctx context.Context, pID NodeID, args *pb2.VoteRequestArgs) (*pb2.VoteResponse, error) {
	client, err := t.GetClient(pID)
	if err != nil {
		return nil, err
	}
	return client.VoteRequest(ctx, args)
}

func (t *Transport) sendLogRequest(ctx context.Context, pID NodeID, args *pb2.LogRequestArgs) (*pb2.LogResponse, error) {
	client, err := t.GetClient(pID)
	if err != nil {
		return nil, err
	}
	return client.LogRequest(ctx, args)
}
