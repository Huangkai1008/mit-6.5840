package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"sync"
	"sync/atomic"
	"time"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	*CommandRequest
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big

	// Your definitions here.
	stateMachine  StateMachine
	lastOperation map[int64]int64
	notifyChanMap map[int]chan *CommandReply
}

func (kv *KVServer) Command(request *CommandRequest, reply *CommandReply) {
	kv.mu.Lock()
	if request.Op != GetOp && kv.isDuplicateRequest(request.ClientId, request.CommandId) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{request})
	Debug(dLog, "S%d start agreement", kv.me)
	if !isLeader {
		Debug(dError, "S%d is not leader", kv.me)
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case result := <-ch:
		reply.Value, reply.Err = result.Value, result.Err
	case <-time.After(CommandTimeout):
		reply.Err = ErrTimeout
	}

	go func() {
		kv.mu.Lock()
		kv.removeOutdatedNotifyCh(index)
		kv.mu.Unlock()
	}()

}

func (kv *KVServer) removeOutdatedNotifyCh(index int) {
	delete(kv.notifyChanMap, index)
}

func (kv *KVServer) isDuplicateRequest(clientId int64, commandId int64) bool {
	if lastCommandId, ok := kv.lastOperation[clientId]; ok && lastCommandId >= commandId {
		return true
	}

	return false
}

func (kv *KVServer) apply(op Op) *CommandReply {
	reply := new(CommandReply)

	switch op.Op {
	case GetOp:
		reply.Value, reply.Err = kv.stateMachine.Get(op.Key)
	case PutOp:
		if !kv.isDuplicateRequest(op.ClientId, op.CommandId) {
			kv.stateMachine.Put(op.Key, op.Value)
			kv.lastOperation[op.ClientId] = op.CommandId
		}
		reply.Err = OK
	case AppendOp:
		if !kv.isDuplicateRequest(op.ClientId, op.CommandId) {
			kv.stateMachine.Append(op.Key, op.Value)
			kv.lastOperation[op.ClientId] = op.CommandId
		}
		reply.Err = OK
	default:
		panic("Unknown operation.")
	}

	return reply
}

func (kv *KVServer) getNotifyCh(index int) chan *CommandReply {
	if _, ok := kv.notifyChanMap[index]; !ok {
		kv.notifyChanMap[index] = make(chan *CommandReply, 1)
	}

	return kv.notifyChanMap[index]
}

func (kv *KVServer) Get(request *CommandRequest, reply *CommandReply) {
	kv.Command(request, reply)
}

func (kv *KVServer) PutAppend(request *CommandRequest, reply *CommandReply) {
	kv.Command(request, reply)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			kv.mu.Lock()
			op := msg.Command.(Op)
			reply := kv.apply(op)
			if currentTerm, isLeader := kv.rf.GetState(); isLeader && msg.CommandTerm == currentTerm {
				index := msg.CommandIndex
				ch := kv.getNotifyCh(index)
				ch <- reply
			}
			kv.mu.Unlock()
		}
	}
}

// StartKVServer start new KV Server, waiting for RPC calls from clerks.
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxRaftState bytes,
// in order to allow Raft to garbage-collect its log. if maxRaftState is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	applyCh := make(chan raft.ApplyMsg)

	kv := &KVServer{
		me:            me,
		rf:            raft.Make(servers, me, persister, applyCh),
		applyCh:       applyCh,
		maxRaftState:  maxRaftState,
		stateMachine:  NewMemoryKV(),
		lastOperation: make(map[int64]int64),
		notifyChanMap: make(map[int]chan *CommandReply),
	}

	go kv.applier()
	return kv
}
