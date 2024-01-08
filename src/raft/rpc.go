package raft

import "fmt"

// RequestVoteRequest is the `RequestVote` RPC arguments structure.
//
// Notes: field names must start with capital letters!
type RequestVoteRequest struct {
	// Term is the candidate's Term.
	Term Term
	// CandidateId is a candidate requesting vote,
	// which is the server id in this lab.
	CandidateId int
	// LastLogIndex is the index of candidate’s last log entry.
	LastLogIndex int
	// LastLogTerm is the term of candidate’s last log entry.
	LastLogTerm Term
}

func (r *RequestVoteRequest) String() string {
	return fmt.Sprintf("Request(CandidateId = %d, LastLogIndex = %d, LastLogTerm = %d, with T%d)",
		r.CandidateId, r.LastLogIndex, r.LastLogTerm, r.Term)
}

// RequestVoteReply is the `RequestVote` RPC reply structure.
//
// Notes: field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).

	// Term is the currentTerm, for a candidate to update itself.
	Term Term
	// VoteGranted is the result of the vote.
	//
	// true means the candidate received the vote,
	// false means follower rejected the vote.
	VoteGranted bool
}

func (r *RequestVoteReply) String() string {
	return fmt.Sprintf("Reply(VoteGranted = %v with T%d)", r.VoteGranted, r.Term)
}

// AppendEntriesRequest is the `AppendEntries` RPC arguments structure.
type AppendEntriesRequest struct {
	// Term is the leader's term.
	Term Term
	// LeaderId, which followers can use this to redirect clients.
	LeaderId int
	// PrevLogIndex is the index of log entry immediately preceding new ones.
	PrevLogIndex int
	// PrevLogTerm is the term of PrevLogIndex entry.
	PrevLogTerm Term
	// Entries to store.
	//
	// Empty for heartbeat; may send more than one for efficiency.
	Entries []Entry
	// LeaderCommit is the leader's commitIndex.
	LeaderCommit int
}

func (r *AppendEntriesRequest) String() string {
	return fmt.Sprintf("Request(PLI = %d, PLT = %d, LeaderCommit = %d, Entries = %v with T%d)",
		r.PrevLogIndex, r.PrevLogTerm, r.LeaderCommit, r.Entries, r.Term)
}

// AppendEntriesReply is the `AppendEntries` RPC reply structure.
type AppendEntriesReply struct {
	// Term, for leader to update itself.
	Term Term
	// Success means whether follower contained entry
	// matching prevLogIndex and prevLogTerm.
	Success bool
	// ConflictTerm is the term of the conflicting entry.
	// -1 means there is no entry in given index.
	ConflictTerm Term
	// ConflictIndex is the first index it stores for ConflictTerm.
	ConflictIndex int
}

func (r *AppendEntriesReply) String() string {
	return fmt.Sprintf("Reply(Success = %v, CT = %d, CI = %d with T%d)",
		r.Success, r.ConflictTerm, r.ConflictIndex, r.Term)
}

// InstallSnapshotRequest is the `InstallSnapshot` RPC request structure.
type InstallSnapshotRequest struct {
	// The leader's Term
	Term Term
	// LeaderId is provided so follower can redirect clients
	LeaderId int
	// The snapshot replaces all entries up through and including LastIncludedIndex
	LastIncludedIndex int
	// The term of lastIncludedIndex
	LastIncludedTerm Term
	// Byte offset where chunk is positioned in the snapshot file
	Offset int
	// Raw bytes of the snapshot chunk, starting at offset
	Data []byte
	// Returns true if it is the last chuck
	Done bool
}

func (r *InstallSnapshotRequest) String() string {
	// TODO: Fill it
	return fmt.Sprintf("Request()")
}

// InstallSnapshotReply is the `InstallSnapshot` RPC reply structure.
type InstallSnapshotReply struct {
	// CurrentTerm, for leader to update itself
	Term Term
}

func (r *InstallSnapshotReply) String() string {
	return fmt.Sprintf("Reply(Term = %d)", r.Term)
}
