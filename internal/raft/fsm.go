package raft

import (
	"fmt"

	"github.com/hardikroongta8/go_raft/internal/utils"
)

func (rf *Node) sendMessageToFSM(msg string) string {
	cmd, err := utils.ParseMessage(msg)
	if err != nil {
		fmt.Printf("[Node %d] Error Parsing Message: %s\n", rf.ID, err.Error())
	}
	switch cmd.(type) {
	case utils.GetCommand:
		key := cmd.(utils.GetCommand).Key
		val := rf.cache.Get(key)
		return fmt.Sprintf("[Node %d] GET: KEY = %s, VALUE = %s", rf.ID, key, val)
	case utils.PutCommand:
		key := cmd.(utils.PutCommand).Key
		val := cmd.(utils.PutCommand).Val
		rf.cache.Put(key, val)
		return fmt.Sprintf("[Node %d] PUT: SUCCESS", rf.ID)
	}
	return fmt.Sprintf("[Node %d] INVALID MESSAGE FOR FSM", rf.ID)
}
