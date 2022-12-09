package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	// Your definitions here.

}

type MUTEX struct {
	mu sync.Mutex
}

var filesall []string
var mapworkerIndex int
var mapServers []string
var reduceIndex int
var finishedWorkers int
var finishedReduceWorkers int
var role int // 0 is wait; 1 is map; 2 is reduce; tell worker what should they do now
var targetmapWorker int
var ret bool
var lock *MUTEX
var reduceNum int

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) SendTasks(args *ExampleArgs, reply *Task) error {
	fmt.Printf("SendTasks!\n")
	lock.mu.Lock()
	reply.Index = reduceIndex
	reduceIndex++
	lock.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveNotify(socket string, reply *Task) error {
	fmt.Printf("ReceiveNotify of map worker done!\n")
	lock.mu.Lock()
	finishedWorkers++
	if finishedWorkers == 4 {
		role = 3
	}
	lock.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveReduceNotify(socket string, reply *Task) error {
	fmt.Printf("ReceiveNotify of reduce worker done!\n")
	lock.mu.Lock()
	finishedReduceWorkers++
	if finishedReduceWorkers == 4 {
		ret = true
	}
	lock.mu.Unlock()
	return nil
}

func (c *Coordinator) DeliverTask(args *ExampleArgs, reply *Task) error {
	fmt.Printf("server: DeliverRole!\n")
	lock.mu.Lock()
	if role == 1 {
		reply.Files = filesall[mapworkerIndex : mapworkerIndex+2]
		reply.Index = mapworkerIndex
		reply.Role = role
		reply.reduceNum = reduceNum
		mapworkerIndex += 2
		if mapworkerIndex == 8 {
			role = 0
		}
		lock.mu.Unlock()
		return nil
	} else if role == 3 {
		reply.Role = role
		reply.Index = reduceIndex
		reduceIndex++
		lock.mu.Unlock()
		return nil
	} else if role == 0 {
		reply.Role = role
		lock.mu.Unlock()
		return nil
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
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
	fmt.Printf("master start listen!\n")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	targetmapWorker = 4
	// Your code here.
	filesall = files
	finishedWorkers = 0
	finishedReduceWorkers = 0
	role = 1
	ret = false
	reduceNum = nReduce
	lock = &MUTEX{}
	c.server()
	return &c
}
