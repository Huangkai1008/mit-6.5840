package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers   []*labrpc.ClientEnd
	clientId  int64
	commandId int64
	leaderId  int64
}

func nRand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func getSvcMeth(op string) string {
	var svcMeth string
	switch op {
	case Put, Append:
		svcMeth = "KVServer.PutAppend"
	case Get:
		svcMeth = "KVServer.Get"
	default:
		panic("Invalid op")
	}
	return svcMeth
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers:   servers,
		clientId:  nRand(),
		commandId: 0,
		leaderId:  0,
	}
}

func (ck *Clerk) newCommandRequest(key, value string, op string) *CommandRequest {
	return &CommandRequest{
		Key:       key,
		Value:     value,
		Op:        op,
		CommandId: ck.commandId,
		ClientId:  ck.clientId,
	}
}

func (ck *Clerk) Command(key string, value string, op string) string {

	request := ck.newCommandRequest(key, value, op)
	svcMeth := getSvcMeth(op)

	for {
		reply := new(CommandReply)

		// If the RPC call is failed or the leader is not the current leader,
		// then try the next server.
		if !ck.servers[ck.leaderId].Call(svcMeth, request, reply) || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}

		// If the RPC call is successful, and the leader is the current leader, make sure
		// the command is executed successfully.
		ck.commandId++
		return reply.Value
	}
}

// Get the current value for a key.
// Returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	return ck.Command(key, "", Get)
}

// PutAppend shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	return ck.Command(key, value, op)
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
