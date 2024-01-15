package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// PutAppendRequest is the structure of `put` or `append` request.
type PutAppendRequest struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetRequest struct {
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	Err   Err
	Value string
}
