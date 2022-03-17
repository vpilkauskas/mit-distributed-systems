package raft

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"time"
)

func (rf *Raft) handleCandidate() {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(rf.candidateVoteTimeoutMaxMS-rf.candidateVoteTimeoutMinMS)))
	electionTimeout := time.Now().Add(time.Duration(int(n.Int64())+rf.candidateVoteTimeoutMinMS) * time.Millisecond)

	request := &RequestVoteArgs{
		Term:        rf.incrTerm(),
		CandidateID: rf.me,
	}

	replies := rf.requestVote(request)

	success := 0
	rf.mu.Lock()
	if rf.serverState != candidate {
		rf.mu.Unlock()
		return
	}
	for i, reply := range replies {
		if i == rf.me || reply == nil {
			continue
		}
		if reply.VoteGranted {
			success++
			continue
		}

		if reply.Term > rf.currentTerm {
			rf.serverState = follower
			rf.currentTerm = reply.Term
			rf.lastHeartbeat = time.Now()
			rf.mu.Unlock()
			fmt.Println(rf.me, "Becoming a follower")
			return
		}
	}

	if success >= rf.requiredVotes {
		rf.serverState = leader
		fmt.Println(rf.me, "Becoming a leader", rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	time.Sleep(time.Until(electionTimeout))
}

func (rf *Raft) requestVote(request *RequestVoteArgs) []*RequestVoteReply {
	replies := make([]*RequestVoteReply, len(rf.peers))
	wg := &sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)

	for peerIndex := range rf.peers {
		if peerIndex == rf.me {
			continue
		}
		go func(index int, replies []*RequestVoteReply, wg *sync.WaitGroup) {
			defer wg.Done()

			reply := &RequestVoteReply{}
			rf.sendRequestVote(index, request, reply)
			replies[index] = reply
		}(peerIndex, replies, wg)
	}
	wg.Wait()
	return replies
}

func waitTimeout(wg *sync.WaitGroup, timeout *time.Timer) bool {
	c := make(chan struct{})
	go func() {
		wg.Wait()
		close(c)
	}()
	select {
	case <-c:
		return false // completed normally
	case <-timeout.C:
		return true // timed out
	}
}
