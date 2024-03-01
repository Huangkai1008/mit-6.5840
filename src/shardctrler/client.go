package shardctrler

//
// Shardctrler clerk.
//

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
	case "Join":
		svcMeth = "ShardCtrler.Join"
	case "Leave":
		svcMeth = "ShardCtrler.Leave"
	case "Move":
		svcMeth = "ShardCtrler.Move"
	case "Query":
		svcMeth = "ShardCtrler.Query"
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

func (ck *Clerk) Command(request *CommandRequest) *CommandReply {
	request.ClientId, request.CommandId = ck.clientId, ck.commandId
	svcMeth := getSvcMeth(request.Op)

	for {
		reply := new(CommandReply)

		Debug(dLog, "C%d -> S%d, %v", ck.clientId, ck.leaderId, request)
		if !ck.servers[ck.leaderId].Call(svcMeth, request, reply) ||
			reply.Err == ErrWrongLeader ||
			reply.Err == ErrTimeout {
			Debug(dLog, "C%d <- S%d, %v", ck.clientId, ck.leaderId, reply)
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
			continue
		}

		Debug(dLog, "C%d <- S%d, %v", ck.clientId, ck.leaderId, reply)
		ck.commandId++
		return reply
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Command(&CommandRequest{
		JoinRequest: JoinRequest{
			Servers: servers,
		},
		Op: JoinOp,
	})

}

func (ck *Clerk) Query(num int) Config {
	reply := ck.Command(&CommandRequest{
		QueryRequest: QueryRequest{Num: num},
		Op:           QueryOp,
	})
	return reply.Config
}

func (ck *Clerk) Leave(gids []int) {
	ck.Command(&CommandRequest{
		LeaveRequest: LeaveRequest{GIDs: gids},
		Op:           LeaveOp,
	})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Command(&CommandRequest{
		MoveRequest: MoveRequest{
			Shard: shard,
			GID:   gid,
		},
		Op: MoveOp,
	})
}
