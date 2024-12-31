package raft

import (
	"context"
	"fmt"
	"github.com/hardikroongta8/go_raft/internal/pb"
)

func (rf *Node) replicateLog(followerID NodeID) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	if rf.currentRole != Leader {
		return
	}
	prefixLen := rf.sentLength[followerID]
	suffix := rf.logs[prefixLen:]
	prefixTerm := int32(0)
	if prefixLen > 0 {
		prefixTerm = rf.logs[prefixLen-1].Term
	}
	args := pb.LogRequestArgs{
		Term:         rf.currentTerm,
		LeaderId:     int32(rf.ID),
		PrefixLen:    prefixLen,
		PrefixTerm:   prefixTerm,
		LeaderCommit: rf.commitedLength,
		Suffix:       suffix,
	}
	res, err := rf.transport.sendLogRequest(context.Background(), followerID, &args)
	if err != nil {
		fmt.Printf("[Node %d] Error while replicating logs: %s\n", rf.ID, err.Error())
		return
	}
	rf.logResponseChannel <- res
}

func (rf *Node) appendEntries(prefixLen int32, leaderCommit int32, suffix []*pb.LogItem) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(suffix) > 0 && int32(len(rf.logs)) > prefixLen {
		index := int32(min(len(rf.logs), int(prefixLen)+len(suffix)) - 1)
		if rf.logs[index].Term != suffix[index-prefixLen].Term {
			rf.logs = rf.logs[:prefixLen]
		}
	}
	if prefixLen+int32(len(suffix)) > int32(len(rf.logs)) {
		for i := int32(len(rf.logs)) - prefixLen; i < int32(len(suffix)); i++ {
			rf.logs = append(rf.logs, suffix[i])
		}
	}
	if leaderCommit > rf.commitedLength {
		for i := rf.commitedLength; i < leaderCommit; i++ {
			res := rf.sendMessageToFSM(rf.logs[i].Message)
			fmt.Println(res)
		}
		rf.commitedLength = leaderCommit
	}
}

func (rf *Node) handleLogResponse(res *pb.LogResponse) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	fmt.Printf("[Node %d] Received Log Response from: Node %d\n", rf.ID, res.FollowerID)
	if res.Term == rf.currentTerm && rf.currentRole == Leader {
		if res.Success && res.AckLen >= rf.ackedLength[NodeID(res.FollowerID)] {
			rf.sentLength[NodeID(res.FollowerID)] = res.AckLen
			rf.ackedLength[NodeID(res.FollowerID)] = res.AckLen
			go rf.commitLogEntries()
			return
		}
		if rf.sentLength[NodeID(res.FollowerID)] > 0 {
			rf.sentLength[NodeID(res.FollowerID)] = rf.sentLength[NodeID(res.FollowerID)] - 1
			go rf.replicateLog(NodeID(res.FollowerID))
			return
		}
	}
	if res.Term > rf.currentTerm {
		rf.currentTerm = res.Term
		rf.currentRole = Follower
		rf.votedFor = -1
		rf.electionTimer.Stop()
	}
}

func (rf *Node) commitLogEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for rf.commitedLength < int32(len(rf.logs)) {
		acks := 0
		for nodeID := range rf.peers {
			if rf.ackedLength[nodeID] > rf.commitedLength {
				acks++
			}
		}
		if acks > len(rf.peers)/2 {
			res := rf.sendMessageToFSM(rf.logs[rf.commitedLength].Message)
			rf.ClientResponseChannel <- res
			rf.commitedLength++
		} else {
			break
		}
	}
}

func (rf *Node) sendHeartbeats() {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	if rf.currentRole == Leader {
		fmt.Printf("[Node %d] Sending Heartbeat\n", rf.ID)
		for followerID := range rf.peers {
			if followerID == rf.ID {
				continue
			}
			go rf.replicateLog(followerID)
		}
	}
}
