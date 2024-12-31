package raft

import (
	"context"
	"fmt"
	"github.com/hardikroongta8/go_raft/internal/pb"
	"github.com/hardikroongta8/go_raft/internal/storage"
	"net"
	"sync"
	"time"
)

type State string
type NodeID int32

const (
	Follower  State = "FOLLOWER"
	Leader    State = "LEADER"
	Candidate State = "CANDIDATE"
)

type Node struct {
	// Persistent
	ID             NodeID
	currentTerm    int32
	votedFor       NodeID
	logs           []*pb.LogItem
	commitedLength int32

	// Volatile
	currentRole   State
	currentLeader NodeID
	votesReceived map[NodeID]bool
	sentLength    map[NodeID]int32
	ackedLength   map[NodeID]int32

	mu        sync.RWMutex
	nodes     map[NodeID]string
	transport *Transport
	cache     *storage.LRUCache

	electionTimer   *time.Timer
	leaderFailTimer *time.Timer

	ClientResponseChannel chan string
	ClientRequestChannel  chan string
	voteResponseChannel   chan *pb.VoteResponse
	logResponseChannel    chan *pb.LogResponse
	quitChannel           chan struct{}

	pb.UnimplementedRaftServer
}

func NewNode(nodes map[NodeID]string, id NodeID) *Node {
	return &Node{
		ID:                    id,
		votedFor:              -1,
		logs:                  make([]*pb.LogItem, 0),
		currentRole:           Follower,
		currentLeader:         -1,
		votesReceived:         make(map[NodeID]bool),
		sentLength:            make(map[NodeID]int32),
		ackedLength:           make(map[NodeID]int32),
		mu:                    sync.RWMutex{},
		nodes:                 nodes,
		transport:             NewTransport(id, nodes),
		voteResponseChannel:   make(chan *pb.VoteResponse),
		ClientRequestChannel:  make(chan string),
		logResponseChannel:    make(chan *pb.LogResponse),
		quitChannel:           make(chan struct{}),
		ClientResponseChannel: make(chan string),
		cache:                 storage.NewLRUCache(100),
	}
}

func (rf *Node) Start(ln net.Listener) {
	go rf.transport.Start(rf, ln)
	go rf.startElectionTimer()
	go rf.startLeaderFailTimer()
	time.Sleep(time.Millisecond * 100)
	heartbeatTimer := time.NewTicker(time.Second * 3)
	defer heartbeatTimer.Stop()

	for {
		select {
		case <-rf.quitChannel:
			return
		case <-rf.leaderFailTimer.C:
			rf.mu.RLock()
			if rf.currentRole != Leader {
				fmt.Printf("[Node %d] Leader Failed to send heartbeat\n", rf.ID)
				go rf.startElection()
			}
			rf.mu.RUnlock()
		case <-rf.electionTimer.C:
			fmt.Printf("[Node %d] Election Timer Expired\n", rf.ID)
			go rf.startElection()
		case <-heartbeatTimer.C:
			go rf.sendHeartbeats()
		case voteRes := <-rf.voteResponseChannel:
			go rf.handleVoteResponse(voteRes)
		case logRes := <-rf.logResponseChannel:
			go rf.handleLogResponse(logRes)
		case msg := <-rf.ClientRequestChannel:
			go rf.handleClientRequest(msg)
		}
	}
}

func (rf *Node) Quit() {
	rf.transport.Stop()
	rf.quitChannel <- struct{}{}
}

func (rf *Node) VoteRequest(ctx context.Context, args *pb.VoteRequestArgs) (*pb.VoteResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[Node %d] Received Vote Request: Node %d\n", rf.ID, args.Id)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentRole = Follower
		rf.votedFor = -1
	}
	lastTerm := int32(0)
	logLen := len(rf.logs)
	if logLen > 0 {
		lastTerm = rf.logs[logLen-1].Term
	}
	logOK := args.LogTerm > lastTerm || (args.LogTerm == lastTerm && args.LogLength >= int32(logLen))

	reply := &pb.VoteResponse{
		Term:    rf.currentTerm,
		Granted: false,
		VoterId: int32(rf.ID),
	}
	if args.Term == rf.currentTerm && logOK && (rf.votedFor == -1 || rf.votedFor == NodeID(args.Id)) {
		rf.votedFor = NodeID(args.Id)
		reply.Granted = true
	}
	return reply, nil
}

func (rf *Node) LogRequest(ctx context.Context, args *pb.LogRequestArgs) (*pb.LogResponse, error) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[Node %d] Received Log Request: Node %d\n", rf.ID, args.LeaderId)
	rf.restartLeaderFailTimer()
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.stopElectionTimer()
	}
	if args.Term == rf.currentTerm {
		rf.currentRole = Follower
		rf.currentLeader = NodeID(args.LeaderId)
	}
	logOK := (len(rf.logs) >= int(args.PrefixLen)) &&
		(args.PrefixLen == 0 || rf.logs[args.PrefixLen-1].Term == args.PrefixTerm)

	res := &pb.LogResponse{
		FollowerID: int32(rf.ID),
		Term:       rf.currentTerm,
		AckLen:     0,
		Success:    false,
	}

	if args.Term == rf.currentTerm && logOK {
		go rf.appendEntries(args.PrefixLen, args.LeaderCommit, args.Suffix)
		res.AckLen = args.PrefixLen + int32(len(args.Suffix))
		res.Success = true
	}
	return res, nil
}

func (rf *Node) handleClientRequest(msg string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[Node %d] Received Message From Client\n", rf.ID)
	if rf.currentRole == Leader {
		rf.logs = append(rf.logs, &pb.LogItem{
			Message: msg,
			Term:    rf.currentTerm,
		})
		rf.ackedLength[rf.ID] = int32(len(rf.logs))
		for followerID := range rf.nodes {
			if followerID == rf.ID {
				continue
			}
			go rf.replicateLog(followerID)
		}
	} else {
		// TODO: Forward Request to rf.currentLeader via a FIFO Link
	}
}

func (rf *Node) startLeaderFailTimer() {
	rf.leaderFailTimer = time.NewTimer(5 * time.Second)
}

func (rf *Node) restartLeaderFailTimer() {
	rf.leaderFailTimer.Reset(time.Second * 5)
}
