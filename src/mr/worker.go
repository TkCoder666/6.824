package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// main/mrworker.go calls this function.
//
func doMapTask(workerId int, taskId int, filename string, nReduce int, mapf func(string, string) []KeyValue) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	maphash := make(map[int][]KeyValue)
	for _, kv := range kva {
		i := ihash(kv.Key) % nReduce
		maphash[i] = append(maphash[i], kv)
	}
	for i := 0; i < nReduce; i++ {
		outfile := tmpOutMapFile(workerId, taskId, i)
		file, err := os.Create(outfile)
		if err != nil {
			log.Fatalf("cannot create %v", outfile)
		}
		for _, kv := range maphash[i] {
			fmt.Fprintf(file, "%v\t%v\n", kv.Key, kv.Value)
		}
		file.Close()
	}

}

func doReduceTask(workerId int, taskId int, nMap int, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	var kv_str_list []string
	for i := 0; i < nMap; i++ {
		filename := finalOutMapFile(i, taskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kv_str_list = append(kv_str_list, strings.Split(string(content), "\n")...)
	}
	for _, kv_str := range kv_str_list {
		if strings.TrimSpace(kv_str) == "" {
			continue
		}
		kv := strings.Split(kv_str, "\t")
		intermediate = append(intermediate, KeyValue{kv[0], kv[1]})
	}

	sort.Sort(ByKey(intermediate))

	oname := tmpOutReduceFile(workerId, taskId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	ofile.Close()

}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	//!we only do tmp thing here,do you know
	WorkerId := os.Getpid()
	LastTaskId := -1
	LastTaskType := ""
	for {
		args := ApplyForTaskArgs{
			WorkerId:     WorkerId,
			LastTaskId:   LastTaskId,
			LastTaskType: LastTaskType,
		}
		reply := ApplyForTaskReply{}
		ok := call("Coordinator.ApplyForTask", &args, &reply) //!many worker may call the Coordinator.ApplyForTask,so lock is important
		if !ok {
			goto END
		}

		switch reply.TaskType {
		case MAP:
			doMapTask(WorkerId, reply.TaskId, reply.FileName, reply.NReduce, mapf)
			// log.Printf("worker %v map task %v done", WorkerId, reply.TaskId)
		case REDUCE:
			doReduceTask(WorkerId, reply.TaskId, reply.NMap, reducef)
			// log.Printf("worker %v reduce task %v done", WorkerId, reply.TaskId)
		case DONE:
			goto END
		}
		LastTaskId = reply.TaskId
		LastTaskType = reply.TaskType
		// log.Printf("worker %v for another task", WorkerId)
	}
END:
	log.Printf("worker %v exit", WorkerId)
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
