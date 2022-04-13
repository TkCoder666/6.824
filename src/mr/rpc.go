package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"os"
	"strconv"
	"time"
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

// Add your RPC definitions here.
//!stage of task and master
const (
	MAP    = "MAP"
	REDUCE = "REDUCE"
	DONE   = "DONE"
)

type Task struct {
	TaskId   int
	WorkerId int
	TaskType string
	DeadLine time.Time
	NMap     int
	NReduce  int
}

type ApplyForTaskArgs struct {
	WorkerId     int
	LastTaskId   int
	LastTaskType string
}

type ApplyForTaskReply struct {
	TaskId   int
	TaskType string
	NMap     int
	NReduce  int
	FileName string
}

func tmpOutMapFile(WorkerId int, MapId int, ReduceId int) string {
	return fmt.Sprint("tmp-map-", WorkerId, "-", MapId, "-", ReduceId)
}

func finalOutMapFile(MapId int, ReduceId int) string {
	return fmt.Sprint("map-out", "-", MapId, "-", ReduceId)
}

func tmpOutReduceFile(WorkerId int, ReduceId int) string {
	return fmt.Sprint("reduce-out", "-", WorkerId, "-", ReduceId)
}

func finalOutReduceFile(ReduceId int) string {
	return fmt.Sprint("mr-out", "-", ReduceId)
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
