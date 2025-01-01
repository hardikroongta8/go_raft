package raft

import (
	"context"
	"fmt"
	"github.com/hardikroongta8/go_raft/internal/pb"
	"math/rand"
	"time"
)

func (rf *Node) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.currentRole = Candidate
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = rf.ID
	rf.votesReceived[rf.ID] = true

	fmt.Printf("[Node %d] Starting election for Term: %d\n", rf.ID, rf.currentTerm)

	lastTerm := int32(0)
	logLen := len(rf.logs)
	if logLen > 0 {
		lastTerm = rf.logs[logLen-1].Term
	}

	for pID := range rf.nodes {
		if pID == rf.ID {
			continue
		}
		args := pb.VoteRequestArgs{
			Id:        int32(rf.ID),
			Term:      rf.currentTerm,
			LogLength: int32(logLen),
			LogTerm:   lastTerm,
		}

		go func(peerID NodeID, args *pb.VoteRequestArgs) {
			reply, err := rf.transport.sendVoteRequest(context.Background(), peerID, args)
			if err != nil {
				fmt.Printf("[Node %d] Error while requesting vote: %s\n", rf.ID, err.Error())
				return
			}
			rf.voteResponseChannel <- reply
		}(pID, &args)
	}
	rf.startElectionTimer()
}

func (rf *Node) handleVoteResponse(res *pb.VoteResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[Node %d] Received Vote Response from: Node %d\n", rf.ID, res.VoterId)
	if rf.currentRole == Candidate && res.Term == rf.currentTerm && res.Granted {
		rf.votesReceived[NodeID(res.VoterId)] = true
		voteCnt := len(rf.votesReceived)
		fmt.Printf("[Node %d] Votes count: %d\n", rf.ID, voteCnt)
		if voteCnt > len(rf.nodes)/2 {
			fmt.Printf("[Node %d] LEADER ELECTED FOR TERM %d\n", rf.ID, res.Term)
			rf.currentRole = Leader
			rf.currentLeader = rf.ID
			rf.stopElectionTimer()
			for followerID := range rf.nodes {
				if followerID == rf.ID {
					continue
				}
				rf.sentLength[followerID] = int32(len(rf.logs))
				rf.ackedLength[followerID] = 0

				go rf.replicateLog(followerID)
			}
		}
	} else if res.Term > rf.currentTerm {
		fmt.Printf("[Node %d] Turning to follower\n", rf.ID)
		rf.currentTerm = res.Term
		rf.currentRole = Follower
		rf.votedFor = -1
		rf.stopElectionTimer()
	}
}

func (rf *Node) handleLeaderFailed() {
	rf.mu.RLock()
	if rf.currentRole != Leader {
		fmt.Printf("[Node %d] Leader Failed to send heartbeat\n", rf.ID)
		go rf.startElection()
	}
	rf.mu.RUnlock()
}

func (rf *Node) startElectionTimer() {
	electionTimeout := time.Duration(3000+rand.Intn(7000)) * time.Millisecond
	fmt.Printf("[Node %d] Election Timeout: %v\n", rf.ID, electionTimeout)
	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(electionTimeout)
	} else {
		rf.electionTimer.Reset(electionTimeout)
	}
}

func (rf *Node) stopElectionTimer() {
	rf.electionTimer.Stop()
}
