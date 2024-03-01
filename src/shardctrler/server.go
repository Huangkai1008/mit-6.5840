package shardctrler

import (
	"6.5840/raft"
	"fmt"
	"sync/atomic"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.

	configs     []Config // indexed by config num
	lastApplied int

	stateMachine  ConfigStateMachine
	lastOperation map[int64]int64
	notifyChanMap map[int]chan *CommandReply
}

type Op struct {
	// Your data here.
	*CommandRequest
}

func (sc *ShardCtrler) Command(request *CommandRequest, reply *CommandReply) {
	sc.mu.Lock()
	if request.Op != QueryOp && sc.isDuplicateRequest(request.ClientId, request.CommandId) {
		reply.Err = OK
		sc.mu.Unlock()
		return
	}
	sc.mu.Unlock()

	index, _, isLeader := sc.rf.Start(Op{request})
	Debug(dLog, "S%d start agreement", sc.me)
	if !isLeader {
		Debug(dError, "S%d is not leader", sc.me)
		reply.Err = ErrWrongLeader
		return
	}

	sc.mu.Lock()
	ch := sc.getNotifyCh(index)
	sc.mu.Unlock()

	select {
	case result := <-ch:
		reply.Config, reply.Err = result.Config, result.Err
	case <-time.After(CommandTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		sc.mu.Lock()
		sc.removeOutdatedNotifyCh(index)
		sc.mu.Unlock()
	}()

}

func (sc *ShardCtrler) removeOutdatedNotifyCh(index int) {
	delete(sc.notifyChanMap, index)
}

func (sc *ShardCtrler) isDuplicateRequest(clientId int64, commandId int64) bool {
	if lastCommandId, ok := sc.lastOperation[clientId]; ok && lastCommandId >= commandId {
		return true
	}

	return false
}

func (sc *ShardCtrler) apply(op Op) *CommandReply {
	reply := new(CommandReply)

	switch op.Op {
	case QueryOp:
		reply.Config, reply.Err = sc.stateMachine.Query(op.Num)
	case JoinOp:
		if !sc.isDuplicateRequest(op.ClientId, op.CommandId) {
			sc.stateMachine.Join(op.Servers)
			sc.lastOperation[op.ClientId] = op.CommandId
		}
		reply.Err = OK
	case LeaveOp:
		if !sc.isDuplicateRequest(op.ClientId, op.CommandId) {
			sc.stateMachine.Leave(op.GIDs)
			sc.lastOperation[op.ClientId] = op.CommandId
		}
		reply.Err = OK
	case MoveOp:
		if !sc.isDuplicateRequest(op.ClientId, op.CommandId) {
			sc.stateMachine.Move(op.Shard, op.GID)
			sc.lastOperation[op.ClientId] = op.CommandId
		}
		reply.Err = OK
	default:
		panic("Unknown operation.")
	}

	return reply
}

func (sc *ShardCtrler) getNotifyCh(index int) chan *CommandReply {
	if _, ok := sc.notifyChanMap[index]; !ok {
		sc.notifyChanMap[index] = make(chan *CommandReply, 1)
	}

	return sc.notifyChanMap[index]
}

func (sc *ShardCtrler) Join(request *CommandRequest, reply *CommandReply) {
	sc.Command(request, reply)
}

func (sc *ShardCtrler) Leave(request *CommandRequest, reply *CommandReply) {
	sc.Command(request, reply)
}

func (sc *ShardCtrler) Move(request *CommandRequest, reply *CommandReply) {
	sc.Command(request, reply)
}

func (sc *ShardCtrler) Query(request *CommandRequest, reply *CommandReply) {
	sc.Command(request, reply)
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		select {
		case msg := <-sc.applyCh:
			if msg.CommandValid {
				sc.mu.Lock()
				if msg.CommandIndex <= sc.lastApplied {
					sc.mu.Unlock()
					continue
				}
				sc.lastApplied = msg.CommandIndex

				op := msg.Command.(Op)
				reply := sc.apply(op)
				if currentTerm, isLeader := sc.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
					index := msg.CommandIndex
					ch := sc.getNotifyCh(index)
					ch <- reply
				}

				sc.mu.Unlock()
			} else {
				panic(fmt.Sprintf("Unexpected message: %v", msg))
			}
		}
	}
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)

	sc := &ShardCtrler{
		me:            me,
		rf:            raft.Make(servers, me, persister, applyCh),
		applyCh:       applyCh,
		lastApplied:   0,
		stateMachine:  newMemoryConfigStateMachine(),
		lastOperation: make(map[int64]int64),
		notifyChanMap: make(map[int]chan *CommandReply),
	}
	go sc.applier()

	return sc
}
