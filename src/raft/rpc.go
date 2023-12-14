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
}

func (r *RequestVoteRequest) String() string {
	return fmt.Sprintf("Request(Term = %d, CandidateId = %d)", r.Term, r.CandidateId)
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
	return fmt.Sprintf("Reply(Term = %d, VoteGranted = %v)", r.Term, r.VoteGranted)
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
	return fmt.Sprintf("Request(Term = %d, LeaderId = %d)", r.Term, r.LeaderId)
}

// AppendEntriesReply is the `AppendEntries` RPC reply structure.
type AppendEntriesReply struct {
	// Term, for leader to update itself.
	Term Term
	// Success means whether follower contained entry
	// matching prevLogIndex and prevLogTerm.
	Success bool
}

func (r *AppendEntriesReply) String() string {
	return fmt.Sprintf("Reply(Term = %d, Success = %v)", r.Term, r.Success)
}
