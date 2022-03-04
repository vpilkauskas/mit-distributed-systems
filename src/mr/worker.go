package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		job := GetJob()
		if job == nil {
			return
		}
		switch job.Type {
		case Map:
			mapFile(job.Files[0], job.ReduceN, mapf)
			JobFinished(job.Files, job.Partition, job.Type)
		case Reduce:
			reduceFiles(job.Files, job.Partition, reducef)
			JobFinished(job.Files, job.Partition, job.Type)
			for _, f := range job.Files {
				os.Remove(getMapFileName(f, job.Partition))
			}
		}
	}
}

func reduceFiles(fileNames []string, partition int, reducef func(string, []string) string) {
	forReduce := map[string][]string{}

	for _, fileName := range fileNames {
		file, err := os.Open(getMapFileName(fileName, partition))
		if err != nil {
			return
		}
		reader := bufio.NewReader(file)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				break
			}
			lineSplit := strings.SplitN(line[:len(line)-1], " ", 2)
			forReduce[lineSplit[0]] = append(forReduce[lineSplit[0]], lineSplit[1])
		}
	}
	dir, _ := os.Getwd()
	outputFile, _ := ioutil.TempFile(dir, "*-")
	for k, v := range forReduce {
		outputFile.Write([]byte(fmt.Sprintf("%v %v\n", k, reducef(k, v))))
	}
	err := os.Rename(outputFile.Name(), fmt.Sprintf("mr-out-%d", partition))
	if err != nil {
		return
	}
}

func mapFile(fileName string, nReduce int, mapf func(string, string) []KeyValue) {
	fmt.Println("starting", fileName)
	defer fmt.Println("ending", fileName)

	start := time.Now()
	content, err := os.ReadFile(fileName)
	if err != nil {
		fmt.Println("failed to read file", fileName, err)
		return
	}
	fmt.Println("Read file ", fileName, "in ", time.Since(start))
	start = time.Now()
	keyValues := mapf(fileName, string(content))
	fmt.Println("Mapf file ", fileName, "in ", time.Since(start))

	dir, _ := os.Getwd()
	outputFiles := []*os.File{}
	for i := 0; i < nReduce; i++ {
		outputF, err := ioutil.TempFile(dir, fmt.Sprintf("*"))
		if err != nil {
			fmt.Println("FAILED TO OPEN TEMP FILE", err)
		}
		outputFiles = append(outputFiles, outputF)
	}

	//output := map[int][]*KeyValue{}

	start = time.Now()
	for _, keyValue := range keyValues {
		_, err := outputFiles[ihash(keyValue.Key)%nReduce].Write(
			[]byte(fmt.Sprintf("%v %v\n", keyValue.Key, keyValue.Value)))
		if err != nil {
			fmt.Println("failed to write to temp file", err)
			return
		}
	}
	fmt.Println("Wrote file ", fileName, "in ", time.Since(start))

	for i, f := range outputFiles {
		err = os.Rename(f.Name(), getMapFileName(fileName, i))
		if err != nil {
			fmt.Println("failed to rename file", err)
			return
		}
		f.Close()
	}
}

func getMapFileName(fileName string, partition int) string {
	return fmt.Sprintf("m-%d-%d", ihash(fileName), partition)
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func GetJob() *JobReply {
	reply := &JobReply{}
	if !call("Coordinator.Job", &JobArgs{}, reply) {
		fmt.Println("exiting")
		return nil
	}

	return reply
}

func JobFinished(files []string, partition int, jobType JobType) {
	//fmt.Println("Ending", jobType)
	call("Coordinator.JobFinished", &JobFinishedArgs{FileNames: files, Partition: partition, Type: jobType}, nil)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
