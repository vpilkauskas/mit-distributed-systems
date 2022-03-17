package raft

import (
	"fmt"
	"time"
)

func (rf *Raft) handleFollower() {
	rf.mu.Lock()
	timeSinceLastHB := time.Since(rf.lastHeartbeat)

	if timeSinceLastHB > rf.electionTimeout {
		rf.serverState = candidate
		fmt.Println(rf.me, "Becoming a candidate", rf.currentTerm)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	time.Sleep(rf.electionTimeout)
}
