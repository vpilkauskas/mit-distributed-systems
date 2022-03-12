package raft

// RequestVoteArgs - represent RequestVote call structure which is used for Leader elections
// This structure is only sent by node who is in Candidate state
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int

	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply - reply to RequestVote command call.
// Possible outcomes:
// * If term >= current term -> return VoteGranted = true
// * If VotedFor == nil or == candidateID and candidate's log is as up-to-date as receiver's log
//		return VoteGranted = true
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}
