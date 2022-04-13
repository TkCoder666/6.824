package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	lock     sync.Mutex
	nMap     int
	nReduce  int
	stage    string
	files    []string
	toDotask chan Task
	tasks    map[string]Task
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) getTaskLabel(tasktype string, taskid int) string {
	return tasktype + "-" + strconv.Itoa(taskid)
}

func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	return nil
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
	ret := false
	ret = (c.stage == DONE)
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:     len(files),
		nReduce:  nReduce,
		stage:    MAP,
		files:    files,
		toDotask: make(chan Task, int(math.Max(float64(len(files)), float64(nReduce)))),
		tasks:    make(map[string]Task),
	}
	for i := 0; i < c.nMap; i++ {
		c.tasks[c.getTaskLabel(MAP, i)] = Task{
			TaskType: MAP,
			TaskId:   i,
			WorkerId: -1,
			NReduce:  nReduce,
			NMap:     c.nMap,
		}
	}
	c.server()

	//!check for time do you know
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)
			c.lock.Lock()
			for _, task := range c.tasks {
				if task.WorkerId != -1 && task.TaskType != DONE && task.DeadLine.After(time.Now()) {
					task.WorkerId = -1
					c.toDotask <- task
					log.Printf("send task %v to dotask with timeout", task)
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}
