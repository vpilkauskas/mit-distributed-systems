package raft

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

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

type state byte

const (
	follower  state = 0x00
	candidate state = 0x01
	leader    state = 0x02
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// serverState - at peer initiation state will always default to follower
	serverState   state
	lastHeartbeat time.Time

	// in other lab assignments these values will be moved to persister
	currentTerm  int
	votedFor     int
	votedForTerm int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// electionTimeout - after this timeout is reached Follower needs to switch to Candidate and start election
	electionTimeout time.Duration
	hbTimeout       time.Duration

	candidateVoteTimeoutMinMS int
	candidateVoteTimeoutMaxMS int

	requiredVotes int
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.serverState == leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
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

func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).

	return true
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if rf.votedForTerm < args.Term {
		rf.votedForTerm = args.Term
		rf.votedFor = args.CandidateID
		rf.lastHeartbeat = time.Now()
		rf.serverState = follower

		reply.Term = args.Term
		reply.VoteGranted = true
		return
	}

	reply.Term = args.Term
	reply.VoteGranted = false
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendRequest(args *AppendRequestArgs, reply *AppendRequestReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	// TODO check if log does not contain an entry at prevLogIndex whose term matches prevLogTerm

	if len(args.Entries) == 0 {
		if rf.serverState != follower {
			rf.serverState = follower
			fmt.Println(rf.me, "Becoming a follower")
		}
		rf.currentTerm = args.Term
		rf.lastHeartbeat = time.Now()

		reply.Success = true
		return
	}
}

func (rf *Raft) sendAppendRequest(server int, args *AppendRequestArgs, reply *AppendRequestReply) bool {
	return rf.peers[server].Call("Raft.AppendRequest", args, reply)
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	return index, term, isLeader
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		switch rf.state() {
		case follower:
			rf.handleFollower()
		case candidate:
			rf.handleCandidate()
		case leader:
			rf.handleLeader()
		}
	}
}

func (rf *Raft) incrTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++

	return rf.currentTerm
}

func (rf *Raft) term() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm
}

func (rf *Raft) setTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.mu.Unlock()
}

func (rf *Raft) state() state {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.serverState
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	hbInterval := time.Second / time.Duration(10/(len(peers)-1))

	rf := &Raft{
		peers:                     peers,
		persister:                 persister,
		me:                        me,
		electionTimeout:           hbInterval + time.Millisecond*100,
		hbTimeout:                 hbInterval,
		candidateVoteTimeoutMinMS: int(hbInterval.Milliseconds()),
		candidateVoteTimeoutMaxMS: int(hbInterval + time.Millisecond*100),
		requiredVotes:             int(math.Floor(float64(len(peers)) / 2)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
