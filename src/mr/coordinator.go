package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.

}

var filesall []string
var mapworkerIndex int
var mapServers []string
var reduceIndex int
var finishedWorkers int
var role int // 0 is wait; 1 is map; 2 is reduce; tell worker what should they do now
var targetmapWorker int

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) SendTasks(args *ExampleArgs, reply *Task) error {
	fmt.Printf("SendTasks!\n")
	reply.Index = reduceIndex
	reduceIndex++
	return nil
}

func (c *Coordinator) ReceiveNotify(socket string, reply *Task) error {
	fmt.Printf("ReceiveNotify of map worker done!\n")
	finishedWorkers++
	if finishedWorkers == 4 {
		role = 3
	}
	return nil
}

func (c *Coordinator) DeliverTask(args *ExampleArgs, reply *Task) error {
	fmt.Printf("DeliverRole!\n")
	if role == 1 {
		reply.Files = filesall[mapworkerIndex : mapworkerIndex+2]
		reply.Index = mapworkerIndex
		reply.Role = role
		mapworkerIndex += 2
		if mapworkerIndex == 8 {
			role = 0
		}
		return nil
	} else if role == 3 {
		reply.Role = role
		reply.Index = reduceIndex
		reduceIndex++
		return nil
	} else if role == 0 {
		reply.Role = role
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
	fmt.Printf("listen!\n")
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

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
	role = 1
	c.server()
	return &c
}
