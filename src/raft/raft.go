package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// State is the raft state.
type State uint8

const (
	Follower State = iota + 1
	Candidate
	Leader
)

// Term acts as a logical clock in Raft.
//
// Term is numbered with consecutive integers.
// Each Term begins with an election.
type Term = int

type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.rand.Intn(n)
}

var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

const (
	HeartBeatInterval = 125
	ElectionTimeout   = 1000
)

func heartBeatInterval() time.Duration {
	return HeartBeatInterval * time.Millisecond
}

func electionTimeout() time.Duration {
	return time.Duration(ElectionTimeout+globalRand.Intn(ElectionTimeout)) * time.Millisecond
}

func winMajority(grantVotes, allVotes int) bool {
	return grantVotes > allVotes/2
}

// Entry contains the Term in which it was created (the number in each box)
// and a Command for the state machine.
//
// Logs are composed of Entry, which are numbered sequentially.
//
// An entry is considered committed if it is safe for that entry to be applied to state machines.
type Entry struct {
	Index   int
	Term    Term
	Command interface{}
}

func (entry Entry) String() string {
	return fmt.Sprintf("Index = %d, Term = %d, Command = %v)", entry.Index, entry.Term, entry.Command)
}

// Raft is a Go object implementing a single Raft peer.
//
// According to the paper's Figure 2, a Raft server must maintain three types of states:
//
// 1. Persistent state on all servers (Updated on stable storage before responding to RPCs).
//
// 2. Volatile state on all servers.
//
// 3. Volatile state on leaders (Reinitialized after election).
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	state State

	// currentTerm is the last Term server has seen.
	//
	// It initialized to 0 on the first boot,
	// increases monotonically.
	currentTerm Term
	// voteFor returns the CandidateId that received vote
	// in the current Term (or null if none)
	// At the beginning, the field is null.
	voteFor int
	// log entries;
	// each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)
	logs []Entry

	// commitIndex is the index of highest log entry known to be committed
	// It initialized to 0, increases monotonically.
	commitIndex int
	// lastApplied is the index of highest log entry applied to state machine
	// It initialized to 0, increases monotonically.
	lastApplied int

	// For each server, index of the next log entry to
	// send to that server (initialized to leader last log index + 1).
	nextIndex []int
	// For each server, index of highest log entry known to
	// be replicated on server (initialized to 0, increases monotonically).
	matchIndex []int

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	heartBeatTimer *time.Timer
	electionTimer  *time.Timer

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// Make creates a new Raft server.
//
// The service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		persister: persister,
		me:        me,
		applyCh:   applyCh,

		state: Follower,

		currentTerm: 0,
		voteFor:     -1,
		logs:        make([]Entry, 1), // dummy log

		commitIndex: 0,
		lastApplied: 0,

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		heartBeatTimer: time.NewTimer(heartBeatInterval()),
		electionTimer:  time.NewTimer(electionTimeout()),
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.applyCond = sync.NewCond(&rf.mu)

	// start ticker goroutine to start elections.
	go rf.ticker()

	// start applier goroutine to apply logs.
	go rf.applier()

	return rf
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.state == Leader
}

func (rf *Raft) isMe(server int) bool {
	return rf.me == server
}

func (rf *Raft) convertTo(state State) {
	rf.state = state

	switch rf.state {
	case Follower:
		Debug(dTimer, "S%d I'm follower, pausing HBT", rf.me)
		rf.heartBeatTimer.Stop()
		rf.electionTimer.Reset(electionTimeout())
	case Candidate:
	case Leader:
		lastEntry := rf.getLastLogEntry()
		// When a leader first comes to power,
		// it initializes all nextIndex values to the index just after the last one in its log.
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = lastEntry.Index + 1
			rf.matchIndex[i] = 0
		}

		rf.electionTimer.Stop()
		rf.heartBeatTimer.Reset(heartBeatInterval())
	}
}

func (rf *Raft) updateTerm(term Term) {
	rf.currentTerm = term
	rf.voteFor = -1
}

func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.voteFor) != nil || e.Encode(rf.logs) != nil {
		Debug(dError, "S% Persist state failed", rf.me)
	}
	return w.Bytes()
}

// save the Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	raftState := rf.encodeRaftState()
	rf.persister.Save(raftState, nil)

	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftState := w.Bytes()
	// rf.persister.Save(raftState, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm Term
	var voteFor int
	var logs []Entry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		Debug(dError, "S% Restores persisted state failed", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
	}

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the first entry is a dummy entry, which index is 0, term is 0.
func (rf *Raft) getFirstLogEntry() Entry {
	return rf.logs[0]
}

func (rf *Raft) getLastLogEntry() Entry {
	return rf.logs[len(rf.logs)-1]
}

// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
func (rf *Raft) advanceCommitForLeader() {
	n := len(rf.matchIndex)
	cmpMatchIndex := make([]int, n)
	copy(cmpMatchIndex, rf.matchIndex)
	sort.Ints(cmpMatchIndex)

	mid := (n - 1) / 2
	newCommitIndex := cmpMatchIndex[mid]
	if newCommitIndex > rf.commitIndex {
		if rf.match(newCommitIndex, rf.currentTerm) {
			Debug(dLog2, "S%d leader advance commit from %d to %d", rf.me, rf.commitIndex, newCommitIndex)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		} else {
			Debug(dLog2, "S%d leader advance commit from %d to %d, but not match", rf.me, rf.commitIndex, newCommitIndex)
		}
	}
}

func (rf *Raft) advanceCommitForFollower(leaderCommit int) {
	if leaderCommit > rf.commitIndex {
		Debug(dLog2, "S%d follower advance commit from %d to %d", rf.me, rf.commitIndex, leaderCommit)
		rf.commitIndex = min(leaderCommit, rf.getLastLogEntry().Index)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) newAppendEntriesRequest(prevLogIndex int) *AppendEntriesRequest {
	entries := make([]Entry, len(rf.logs[prevLogIndex+1:]))
	copy(entries, rf.logs[prevLogIndex+1:])

	return &AppendEntriesRequest{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.logs[prevLogIndex].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func (rf *Raft) sendAppendEntries(peer int, request *AppendEntriesRequest, reply *AppendEntriesReply) bool {
	ok := rf.peers[peer].Call("Raft.AppendEntries", request, reply)
	return ok
}

// Raft determines which of two logs is more up-to-date by
// comparing the index and term of the last entries in the logs.
//
// If the logs have last entries with different terms,
// then the log with the later term is more up-to-date.
// If the logs end with the same term,
// then whichever log is longer is more up-to-date.
func (rf *Raft) isUpToDate(logIndex int, term Term) bool {
	lastLogEntry := rf.getLastLogEntry()
	return term > lastLogEntry.Term || (term == lastLogEntry.Term && logIndex >= lastLogEntry.Index)
}

// match returns whether the follower finds an entry in its log
// with the same index and term.
//
// According to the `Log Matching Property`:
// if two logs contain an entry with the same index and term,
// then the logs are identical in all entries up through the given index.
func (rf *Raft) match(logIndex, logTerm int) bool {
	return logIndex <= rf.getLastLogEntry().Index && rf.logs[logIndex].Term == logTerm
}

func (rf *Raft) handleAppendEntriesReply(peer int, request *AppendEntriesRequest, reply *AppendEntriesReply) {
	// If the reply is outdated, ignore it.
	if !rf.isLeader() || rf.currentTerm != request.Term {
		return
	}

	if reply.Success {
		rf.matchIndex[peer] = request.PrevLogIndex + len(request.Entries)
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommitForLeader()
		return
	}

	if rf.currentTerm < reply.Term {
		rf.updateTerm(reply.Term)
		rf.convertTo(Follower)
	} else if rf.currentTerm == reply.Term {
		rf.nextIndex[peer] = reply.ConflictIndex
		if reply.ConflictTerm != -1 {
			for index := request.PrevLogIndex; index >= 0; index-- {
				if rf.logs[index].Term == reply.ConflictTerm {
					rf.nextIndex[peer] = index + 1
					break
				}
			}
		}
	}
}

// broadcastAppendEntries sends AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
//
// It is also used as heartbeat.
// TODO: replicate in batch
func (rf *Raft) broadcastAppendEntries(isHeartBeat bool) {
	if isHeartBeat {
		Debug(dTimer, "S%d Leader, sending heartbeat.", rf.me)
	} else {
		Debug(dTimer, "S%d Leader, replicating entries.", rf.me)
	}

	for peer := range rf.peers {
		if rf.isMe(peer) {
			continue
		}

		go func(peer int) {
			rf.mu.RLock()
			if !rf.isLeader() {
				rf.mu.RUnlock()
				return
			}

			prevLogIndex := rf.nextIndex[peer] - 1
			request := rf.newAppendEntriesRequest(prevLogIndex)
			rf.mu.RUnlock()

			reply := new(AppendEntriesReply)
			Debug(
				dLog, "S%d -> S%d, AE: %v", rf.me, peer, request,
			)

			if rf.sendAppendEntries(peer, request, reply) {
				Debug(dLog, "S%d <- S%d, AE: %v", rf.me, peer, reply)

				rf.mu.Lock()
				rf.handleAppendEntriesReply(peer, request, reply)
				rf.mu.Unlock()
			}

		}(peer)
	}
}

// AppendEntries RPC handler
//
// Which is invoked by leader to replicate log entries (§5.3); also used as heartbeat (§5.2).
func (rf *Raft) AppendEntries(request *AppendEntriesRequest, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If a server receives a request with a stale term number,
	// it rejects the request.
	if request.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If RPC request or response contains Term T > currentTerm:
	// set currentTerm = T, convert to follower.
	//
	// If the leader’s term (included in its RPC) is at least
	// as large as the candidate’s current term,
	// then the candidate recognizes the leader as legitimate and
	// returns to follower state
	if request.Term > rf.currentTerm {
		rf.updateTerm(request.Term)
	}

	rf.convertTo(Follower)

	// If the follower does not find an entry in its log with the same index and term,
	// then it refuses the new entries.
	if !rf.match(request.PrevLogIndex, request.PrevLogTerm) {
		lastIndex := rf.getLastLogEntry().Index
		if lastIndex < request.PrevLogIndex {
			reply.ConflictTerm = -1
			reply.ConflictIndex = lastIndex + 1
		} else {
			reply.ConflictTerm = rf.logs[request.PrevLogIndex].Term
			index := request.PrevLogIndex - 1
			for index >= 0 && rf.logs[index].Term == reply.ConflictTerm {
				index--
			}
			reply.ConflictIndex = index
		}

		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// Replicate logs.
	//
	// Notes: If there are no conflicting logs, do not delete any follower logs, as follower logs may be more recent.
	for index, entry := range request.Entries {
		// Find the latest log entry where the two logs agree,
		// delete any entries in the follower’s log after that point,
		// and send the follower all of the leader’s entries after that point.
		if entry.Index > rf.getLastLogEntry().Index || rf.logs[entry.Index].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index], request.Entries[index:]...)
			break
		}
	}

	rf.advanceCommitForFollower(request.LeaderCommit)

	reply.Term = rf.currentTerm
	reply.Success = true
	Debug(dTimer, "S%d received S%d heartbeat at T%d", rf.me, request.LeaderId, rf.currentTerm)
}

func (rf *Raft) newRequestVoteRequest() *RequestVoteRequest {
	lastLogEntry := rf.getLastLogEntry()

	return &RequestVoteRequest{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus, there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, request *RequestVoteRequest, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", request, reply)
	return ok
}

// RequestVote RPC handler,
//
// which is invoked by candidates to gather votes.
func (rf *Raft) RequestVote(request *RequestVoteRequest, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If the candidate's Term is smaller than the current Term,
	// reject the vote and return the current Term.
	if request.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// Each server will vote for at most one candidate in a given term, on a first-come-first-served basis.
	if request.Term == rf.currentTerm && rf.voteFor != -1 && rf.voteFor != request.CandidateId {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	// If RPC request or response contains Term T > currentTerm:
	// set currentTerm = T, convert to follower.
	if request.Term > rf.currentTerm {
		Debug(
			dTerm, "S%d Term is higher than S%d, updating (%d > %d)",
			request.CandidateId, rf.me, request.Term, rf.currentTerm,
		)

		rf.updateTerm(request.Term)
	}

	// Election restriction (§5.4.1)
	//
	// The voter denies its vote if its own log is more up-to-date than
	// that of the candidate.
	if !rf.isUpToDate(request.LastLogIndex, request.LastLogTerm) {
		Debug(dLog2, "S%d Reject S%d vote, not up-to-update", rf.me, request.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	rf.convertTo(Follower)
	rf.voteFor = request.CandidateId
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	Debug(dVote, "S%d Granting Vote to S%d at T%d", rf.me, request.CandidateId, rf.currentTerm)
}

func (rf *Raft) appendNewEntry(command interface{}) Entry {
	lastLogEntry := rf.getLastLogEntry()
	entry := Entry{
		Index:   lastLogEntry.Index + 1,
		Term:    rf.currentTerm,
		Command: command,
	}

	rf.logs = append(rf.logs, entry)
	return entry
}

// Start agreement on a new log entry.
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. If this
// server isn't the leader, returns false. Otherwise, start the
// agreement and return immediately. There is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. Even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. The second return value is the current
// Term. The third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader() {
		return -1, rf.currentTerm, false
	}

	entry := rf.appendNewEntry(command)
	Debug(dClient, "S%d received command %v", rf.me, entry)

	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = entry.Index, entry.Index+1
	rf.broadcastAppendEntries(false)
	return entry.Index, entry.Term, true
}

// Snapshot call with a serialized snapshot of its state.
//
// The service says it has created a snapshot that has
// all info up to and including index.
// This means the service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	firstIndex := rf.getFirstLogEntry().Index
	if index <= firstIndex {
		
	}

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// startElection invoked when election timeout elapses
// without receiving AppendEntries RPC from
// the current leader or granting vote to candidate.
func (rf *Raft) startElection() {
	// On conversion to candidate, start election:
	// • Increment currentTerm
	// • Vote for self
	// • Reset election timer
	// • Send RequestVote RPCs to all other servers
	rf.convertTo(Candidate)
	rf.currentTerm++
	rf.voteFor = rf.me

	Debug(dTimer, "S%d Resetting election timeout because election", rf.me)
	rf.electionTimer.Reset(electionTimeout())
	grantVotes := 1

	Debug(dTerm, "S%d Converting to Candidate, calling election in T%d", rf.me, rf.currentTerm)

	request := rf.newRequestVoteRequest()

	for peer := range rf.peers {
		if rf.isMe(peer) {
			continue
		}

		go func(peer int) {
			reply := new(RequestVoteReply)
			Debug(dLog, "S%d -> S%d, RV: %v", rf.me, peer, request)

			if rf.sendRequestVote(peer, request, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				Debug(dLog, "S%d <- S%d, RV: %v", rf.me, peer, reply)

				if rf.currentTerm == request.Term && rf.state == Candidate {
					if reply.VoteGranted {
						Debug(dVote, "S%d <- S%d Got vote", rf.me, peer)

						grantVotes++
						if winMajority(grantVotes, len(rf.peers)) {
							Debug(
								dLeader, "S%d Achieved Majority for T%d (%d/%d), converting to Leader",
								rf.me, rf.currentTerm, grantVotes, len(rf.peers),
							)
							rf.convertTo(Leader)
							rf.broadcastAppendEntries(true)
							// Once a candidate wins an election, it becomes leader.
							// It then sends heartbeat messages to all of the other servers
							// to establish its authority and prevent new elections.
						}
					} else if rf.currentTerm < reply.Term {
						rf.updateTerm(reply.Term)
						rf.convertTo(Follower)
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		select {
		case <-rf.heartBeatTimer.C:
			rf.mu.Lock()
			if rf.isLeader() {
				rf.broadcastAppendEntries(true)
				rf.heartBeatTimer.Reset(heartBeatInterval())
			}
			rf.mu.Unlock()
		case <-rf.electionTimer.C:
			if !rf.isLeader() {
				Debug(dTimer, "S%d Not Leader, checking election timeout", rf.me)
				rf.mu.Lock()
				rf.startElection()
				rf.mu.Unlock()
			}
		}
	}
}

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			Debug(dCommit, "S%d Waiting for commitIndex to be updated (%d/%d)", rf.me, rf.lastApplied, rf.commitIndex)
			rf.applyCond.Wait()
		}

		// If commitIndex > lastApplied: increment lastApplied, apply log[lastApplied] to state machine.
		commitIndex, lastApplied := rf.commitIndex, rf.lastApplied
		entries := make([]Entry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied+1:commitIndex+1])
		rf.mu.Unlock()

		for _, entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
		}

		rf.mu.Lock()
		rf.lastApplied = max(rf.lastApplied, commitIndex)
		rf.mu.Unlock()
	}
}
