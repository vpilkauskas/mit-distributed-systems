package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type JobArgs struct {
}

type JobType byte

const (
	Map    JobType = 0x01
	Reduce JobType = 0x02
)

type JobReply struct {
	Type      JobType
	Files     []string
	ReduceN   int
	Partition int
}

type JobFinishedArgs struct {
	Type      JobType
	FileNames []string
	Partition int
}

type JobFinishedReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
