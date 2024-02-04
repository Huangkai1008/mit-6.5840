package kvraft

import (
	"fmt"
	"time"
)

const (
	OK             Err = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrTimeout         = "ErrTimeout"
)

type Err string

const (
	PutOp    = "Put"
	AppendOp = "Append"
	GetOp    = "Get"
)

const CommandTimeout = 500 * time.Millisecond

// CommandRequest is the structure of command request.
// If Op is "Put" or "Append", then Value is the value to put or append.
// If Op is "Get", then the Value is empty.
type CommandRequest struct {
	Key       string
	Value     string
	Op        string
	CommandId int64
	ClientId  int64
}

func (r *CommandRequest) String() string {
	return fmt.Sprintf("Request(Op = %s, Key = %s, Value = %s, CommandId = %d, ClientId = %d)",
		r.Op, r.Key, r.Value, r.CommandId, r.ClientId)
}

type CommandReply struct {
	Err Err

	// If the Op is "Get", then the Value is the value of the key.
	// If the Op is "Put" or "Append", then the Value is empty.
	Value string
}

func (r *CommandReply) String() string {
	return fmt.Sprintf("Reply(Err = %s, Value = %s)", r.Err, r.Value)
}

type GetRequest struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
