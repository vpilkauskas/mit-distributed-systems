package raft

import (
	"log"
	"math"
	"math/rand"
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
	currentTerm int
	votedFor    *int

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	// heartBeatTimeout - after this timeout is reached Follower needs to switch to Candidate and start election
	heartBeatTimeout time.Duration

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
		return
	}

	if rf.votedFor != nil && *rf.votedFor != args.CandidateID && args.Term == rf.currentTerm {
		return
	}
	rf.votedFor = &args.CandidateID
	rf.currentTerm = args.Term
	rf.lastHeartbeat = time.Now()

	reply.VoteGranted = true
	return
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) AppendRequest(args *AppendRequestArgs, reply *AppendRequestReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	if args.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return
	}
	// TODO check if log does not contain an entry at prevLogIndex whose term matches prevLogTerm

	rf.mu.Lock()
	rf.lastHeartbeat = time.Now()
	rf.mu.Unlock()
	if len(args.Entries) == 0 {
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

func (rf *Raft) handleFollower() {
	rf.mu.Lock()
	lastHB := rf.lastHeartbeat
	rf.mu.Unlock()

	timeSinceLastHB := time.Since(lastHB)

	if timeSinceLastHB > rf.heartBeatTimeout {
		rf.setState(candidate)
		return
	}
	time.Sleep(rf.heartBeatTimeout - timeSinceLastHB)
}

func (rf *Raft) handleCandidate() {
	log.Println("Executing candidate flow")
	electionTimeout := rand.Intn(rf.candidateVoteTimeoutMaxMS-rf.candidateVoteTimeoutMinMS) + rf.candidateVoteTimeoutMinMS
	electionTimeoutTimer := time.NewTimer(time.Duration(electionTimeout) * time.Millisecond)
	defer electionTimeoutTimer.Stop()

	term := rf.incrTerm()

	request := &RequestVoteArgs{
		Term:        term,
		CandidateID: rf.me,
	}

	replyC := make(chan *RequestVoteReply)
	for peerIndex := range rf.peers {
		if peerIndex == rf.me {
			continue
		}

		go func(pIndex int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(pIndex, request, reply)

			select {
			case <-electionTimeoutTimer.C:
				return
			case replyC <- reply:
			}

		}(peerIndex)
	}

	success := 0
	for i := 0; i < len(rf.peers)-1; i++ {
		select {
		case <-electionTimeoutTimer.C:
			return
		case reply := <-replyC:
			if reply.VoteGranted {
				success++
				if success < rf.requiredVotes {
					continue
				}
				rf.setState(leader)
				return
			}
			if reply.Term > rf.currentTerm {
				rf.setState(follower)
				return
			}
		}
	}

}

func (rf *Raft) handleLeader() {
	log.Println("Executing leader flow")
	heartbeatTicker := time.NewTicker(time.Millisecond * 100)
	defer heartbeatTicker.Stop()

	term, success := rf.sendHeartbeat()
	if !success {
		rf.setState(follower)
		rf.setTerm(term)
		return
	}

	for range heartbeatTicker.C {
		term, success := rf.sendHeartbeat()
		if success {
			rf.setState(follower)
			rf.setTerm(term)
			return
		}
	}
}

func (rf *Raft) sendHeartbeat() (term int, success bool) {
	request := &AppendRequestArgs{
		Term:     rf.term(),
		LeaderID: rf.me,
	}

	appendReplyC := make(chan *AppendRequestReply)
	for peerIndex := range rf.peers {
		go func(index int) {
			reply := &AppendRequestReply{}
			rf.sendAppendRequest(index, request, reply)
			appendReplyC <- reply
		}(peerIndex)
	}

	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-appendReplyC
		if reply.Success {
			continue
		}

		if reply.Term > request.Term {
			rf.setTerm(reply.Term)
			return reply.Term, false
		}
	}
	return 0, true
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

func (rf *Raft) setState(s state) {
	rf.mu.Lock()
	rf.serverState = s
	rf.mu.Unlock()
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:                     peers,
		persister:                 persister,
		me:                        me,
		heartBeatTimeout:          time.Millisecond * 150,
		candidateVoteTimeoutMinMS: 150,
		candidateVoteTimeoutMaxMS: 300,
		requiredVotes:             int(math.Ceil(float64(len(peers)-1) / 2)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
