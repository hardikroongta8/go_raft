package utils

import (
	"fmt"
)

type Command interface{}

type PutCommand struct {
	Key, Val string
}
type GetCommand struct {
	Key string
}

func ParseMessage(msg string) (Command, error) {
	cmd := deserializeString(msg)
	if len(cmd) == 3 && cmd[0] == "PUT" {
		return PutCommand{
			Key: cmd[1],
			Val: cmd[2],
		}, nil
	}
	if len(cmd) == 2 && cmd[0] == "GET" {
		return GetCommand{Key: cmd[1]}, nil
	}
	return nil, fmt.Errorf("INVALID MESSAGE: %s", msg)
}

func deserializeString(msg string) []string {
	cmd := make([]string, 0)
	lf := 0
	for i := 0; i < len(msg)-1; i++ {
		if msg[i] == '\r' && msg[i+1] == '\n' {
			cmd = append(cmd, msg[lf:i])
			lf = i + 2
			i++
		}
	}
	return cmd
}
