package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	originalFiles []string
	files         []string
	mapSchedule   []*fileMapProgress

	preReducedFiles map[int]struct{}
	reduceSchedule  []*partitionReduceProgress

	mu      sync.RWMutex
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.
type fileMapProgress struct {
	name        string
	scheduledAt time.Time
}

type partitionReduceProgress struct {
	files           []string
	reducePartition int
	scheduledAt     time.Time
}

func (c *Coordinator) Job(_ *JobArgs, reply *JobReply) error {
	var file *string
	file = c.getFileForMapping()
	for c.hasFilesForMapping() && file == nil {
		file = c.getFileForMapping()
		time.Sleep(time.Millisecond * 100)
	}
	if file != nil {
		reply.Type = Map
		reply.ReduceN = c.nReduce
		reply.Files = append(reply.Files, *file)
		return nil
	}

	// Reduce time
	var files []string
	var partition int
	files, partition = c.getFilesForReduce()
	for c.hasFilesForReduce() && files == nil {
		files, partition = c.getFilesForReduce()
		time.Sleep(time.Millisecond * 100)
	}
	if files == nil {
		return fmt.Errorf("no more jobs")
	}

	reply.Type = Reduce
	reply.ReduceN = c.nReduce
	reply.Files = files
	reply.Partition = partition
	return nil
}

func (c *Coordinator) JobFinished(args *JobFinishedArgs, _ *JobFinishedArgs) error {
	switch args.Type {
	case Map:
		c.mapFinished(args.FileNames[0])
	case Reduce:
		c.reduceFinished(args.Partition)
	}

	return nil
}

func (c *Coordinator) hasFilesForMapping() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.files) != 0 || len(c.mapSchedule) != 0
}

func (c *Coordinator) hasFilesForReduce() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.preReducedFiles) != 0 || len(c.reduceSchedule) != 0
}

func (c *Coordinator) mapFinished(filename string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, v := range c.mapSchedule {
		if v.name != filename {
			continue
		}
		c.mapSchedule = append(c.mapSchedule[:i], c.mapSchedule[i+1:]...)
	}
}

func (c *Coordinator) reduceFinished(partition int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, v := range c.reduceSchedule {
		if v.reducePartition != partition {
			continue
		}
		c.reduceSchedule = append(c.reduceSchedule[:i], c.reduceSchedule[i+1:]...)
	}
}

func (c *Coordinator) getFileForMapping() *string {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.files) == 0 && len(c.mapSchedule) == 0 {
		return nil
	}

	scheduled := len(c.mapSchedule)
	if scheduled != 0 && c.mapSchedule[0].scheduledAt.Add(time.Second*10).Before(time.Now()) {
		c.mapSchedule = append(c.mapSchedule, &fileMapProgress{
			name:        c.mapSchedule[0].name,
			scheduledAt: time.Now(),
		})

		c.mapSchedule = c.mapSchedule[1:]
		return &c.mapSchedule[len(c.mapSchedule)-1].name
	}
	if len(c.files) == 0 {
		return nil
	}

	c.mapSchedule = append(c.mapSchedule, &fileMapProgress{
		name:        c.files[0],
		scheduledAt: time.Now(),
	})

	c.files = c.files[1:]
	return &c.mapSchedule[len(c.mapSchedule)-1].name
}

func (c *Coordinator) getFilesForReduce() ([]string, int) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if len(c.preReducedFiles) == 0 && len(c.reduceSchedule) == 0 {
		return nil, -1
	}

	scheduled := len(c.reduceSchedule)
	if scheduled != 0 && c.reduceSchedule[0].scheduledAt.Add(time.Second*10).Before(time.Now()) {
		c.reduceSchedule = append(c.reduceSchedule, &partitionReduceProgress{
			files:           c.reduceSchedule[0].files,
			reducePartition: c.reduceSchedule[0].reducePartition,
			scheduledAt:     time.Now(),
		})

		c.reduceSchedule = c.reduceSchedule[1:]
		return c.reduceSchedule[len(c.reduceSchedule)-1].files, c.reduceSchedule[len(c.reduceSchedule)-1].reducePartition
	}
	if len(c.preReducedFiles) == 0 {
		return nil, -1
	}

	for k, _ := range c.preReducedFiles {
		c.reduceSchedule = append(c.reduceSchedule, &partitionReduceProgress{
			files:           c.originalFiles,
			reducePartition: k,
			scheduledAt:     time.Now(),
		})
		delete(c.preReducedFiles, k)
		break
	}
	return c.reduceSchedule[len(c.reduceSchedule)-1].files, c.reduceSchedule[len(c.reduceSchedule)-1].reducePartition
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	return len(c.files) == 0 && len(c.mapSchedule) == 0 && len(c.preReducedFiles) == 0 && len(c.reduceSchedule) == 0
}

func (c *Coordinator) loadReduceFiles(nReduce int, files []string) {
	for i := 0; i < nReduce; i++ {
		c.preReducedFiles[i] = struct{}{}
	}
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{preReducedFiles: map[int]struct{}{}}
	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.originalFiles = files
	c.loadReduceFiles(nReduce, files)
	c.server()
	return &c
}
