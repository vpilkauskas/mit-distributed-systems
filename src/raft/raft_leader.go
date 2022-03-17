package raft

import (
	"fmt"
	"sync"
	"time"
)

func (rf *Raft) handleLeader() {
	nextHeartbeat := time.Now().Add(rf.hbTimeout)
	request := &AppendRequestArgs{
		Term:     rf.term(),
		LeaderID: rf.me,
	}
	replies := rf.doHeartbeat(request)

	rf.mu.Lock()
	if rf.serverState != leader {
		rf.mu.Unlock()
		return
	}

	success := 0
	for i, reply := range replies {
		if i == rf.me || reply == nil {
			continue
		}
		if reply.Success {
			success++
			continue
		}
		if reply.Term > rf.currentTerm {
			rf.lastHeartbeat = time.Now()
			rf.serverState = follower
			rf.currentTerm = reply.Term
			rf.mu.Unlock()
			fmt.Println(rf.me, "Becoming a follower")
			return
		}
	}
	if success < rf.requiredVotes {
		rf.serverState = follower
		rf.lastHeartbeat = time.Now()
		fmt.Println(rf.me, "Becoming a follower", success, rf.requiredVotes)
	}
	rf.mu.Unlock()
	time.Sleep(time.Until(nextHeartbeat))
}

func (rf *Raft) doHeartbeat(request *AppendRequestArgs) []*AppendRequestReply {
	replies := make([]*AppendRequestReply, len(rf.peers))
	wg := &sync.WaitGroup{}
	wg.Add(len(rf.peers) - 1)

	for peerIndex := range rf.peers {
		if peerIndex == rf.me {
			continue
		}
		go func(index int, replies []*AppendRequestReply, wg *sync.WaitGroup) {
			reply := &AppendRequestReply{}
			rf.sendAppendRequest(index, request, reply)
			replies[index] = reply
			wg.Done()
		}(peerIndex, replies, wg)
	}

	wg.Wait()
	return replies
}
