package raft

//AppendRequestArgs - used by Leader to:
//- replicate log entries
//- heartbeat for followers
//Implementation notes:
//- If an existing entry conflicts with a new one (same index
//	 but different terms), delete the existing entry and all that
//  follow it
//- Append any new entries not already in the log
//- If leaderCommit > commitIndex, set commitIndex =
//  min(leaderCommit, index of last new entry)
type AppendRequestArgs struct {
	// Term - Leader's Term
	Term int

	// LeaderID - Used by followers to redirect Clients
	LeaderID int

	// PervLogIndex - Index of log entry immediately preceding new ones
	PervLogIndex int

	// PrevLogTerm - Term of previous log index entry
	PrevLogTerm int

	// Entries - Log entries to store
	// If entries are empty - peers need to look into it as a heartbeat
	Entries []string

	// LeadersCommit - Leaders commit index
	LeadersCommit int
}

// AppendRequestReply - reply to append request call
type AppendRequestReply struct {
	// Term - Current Term for leader to update itself
	Term int

	// Success - Indicates if an append request call was a success
	// Scenarios:
	// - If term < current peer term -> return false
	// - If log doesn't contain an entry at PrevLogIndex whose term matches PrevLogTerm -> return false
	Success int
}
