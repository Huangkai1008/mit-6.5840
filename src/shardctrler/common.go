package shardctrler

import (
	"fmt"
	"time"
)

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10
const CommandTimeout = 500 * time.Millisecond

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

const (
	JoinOp  = "Join"
	LeaveOp = "Leave"
	MoveOp  = "Move"
	QueryOp = "Query"
)

type CommandRequest struct {
	JoinRequest
	LeaveRequest
	MoveRequest
	QueryRequest

	Op        string
	CommandId int64
	ClientId  int64
}

func (c *CommandRequest) String() string {
	switch c.Op {
	case JoinOp:
		return fmt.Sprintf("JoinOpRequest(Servers = %v, CommandId = %d, ClientId = %d)",
			c.Servers, c.CommandId, c.ClientId,
		)
	case LeaveOp:
		return fmt.Sprintf("LeaveOpRequest(GIDs = %v, CommandId = %d, ClientId = %d)",
			c.GIDs, c.CommandId, c.ClientId,
		)
	case MoveOp:
		return fmt.Sprintf("MoveOpRequest(Shard = %d, GID = %d, CommandId = %d, ClientId = %d)",
			c.Shard, c.GID, c.CommandId, c.ClientId,
		)
	case QueryOp:
		return fmt.Sprintf("QueryOpRequest(Num = %d, CommandId = %d, ClientId = %d)",
			c.Num, c.CommandId, c.ClientId,
		)
	default:
		return fmt.Sprintf("UnknownOpRequest(CommandId = %d, ClientId = %d, Op = %s)",
			c.CommandId, c.ClientId, c.Op,
		)
	}
}

type CommandReply struct {
	Err Err

	// The configuration number of the configuration that the command was applied to.
	// Only present in the reply to a QueryOp.
	Config Config
}

func (c *CommandReply) String() string {
	return fmt.Sprintf("Reply(Err = %s, Config = %v)", c.Err, c.Config)
}

type JoinRequest struct {
	// A set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names,
	Servers map[int][]string
}

type LeaveRequest struct {
	// A set of unique, non-zero replica group identifiers (GIDs).
	GIDs []int
}

type MoveRequest struct {
	// A shard number and a replica group identifier (GID).
	Shard int
	GID   int
}

type QueryRequest struct {
	// A desired configuration number.
	Num int
}
