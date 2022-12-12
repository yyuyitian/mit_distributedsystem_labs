package mr

import (
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

}

type MUTEX struct {
	mu sync.Mutex
}

var filesall []string
var fileIndex int
var mapServers []string
var reduceIndex int
var finishedfiles int
var finishedReduceWorkers int
var role int // 0 is wait; 1 is map; 2 is reduce; tell worker what should they do now
var ret bool
var lock *MUTEX
var reduceNum int
var filesSize int              // size of files to be deal with
var failedfiles []int          // files do not dealed within 10s
var failedReduceTasks []int    // reduce task do not dealed within 10s
var deliveredMapTask []bool    // file index have been delivered
var deliveredReduceTask []bool // task index have been delivered
const checkTimes int = 5
const sleepTime int = 1 // 1s

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) ReceiveNotify(socket string, reply *Task) error {
	//fmt.Printf("ReceiveNotify of map worker finish a file done!\n")
	lock.mu.Lock()
	finishedfiles++
	if finishedfiles == filesSize {
		role = ReduceWorker
	}
	lock.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveReduceNotify(socket string, reply *Task) error {
	//fmt.Printf("ReceiveNotify of reduce worker done!\n")
	lock.mu.Lock()
	finishedReduceWorkers++
	if finishedReduceWorkers == reduceNum {
		ret = true
	}
	lock.mu.Unlock()
	return nil
}

func (c *Coordinator) DeliverTask(args *ExampleArgs, reply *Task) error {
	//fmt.Printf("server: DeliverRole!\n")
	lock.mu.Lock()
	if role == MapWorker {
		// first deal with the failed files, when
		if len(failedfiles) > 0 {
			index := failedfiles[len(failedfiles)-1] // index of failed file
			reply.File = filesall[index]
			reply.Role = role
			reply.ReduceNum = reduceNum
			go checkMapWorker(index)
			failedfiles = failedfiles[:len(failedfiles)-1] // remove delivered task
			if len(failedfiles) == 0 && fileIndex == filesSize {
				role = WaitToWork
			}
			lock.mu.Unlock()
			return nil
		}
		reply.File = filesall[fileIndex]
		reply.Role = role
		reply.ReduceNum = reduceNum
		go checkMapWorker(fileIndex)
		fileIndex++
		if fileIndex == filesSize {
			role = WaitToWork
		}
		lock.mu.Unlock()
		return nil
	} else if role == ReduceWorker {
		if reduceIndex >= reduceNum {
			reply.Role = WaitToWork
			lock.mu.Unlock()
			return nil
		}
		if len(failedReduceTasks) > 0 {
			reply.Index = failedReduceTasks[len(failedReduceTasks)-1]
			reply.Role = role
			if len(failedReduceTasks) == 0 && reduceIndex >= reduceNum {
				reply.Role = WaitToWork
			}
			lock.mu.Unlock()
			return nil
		}
		reply.Role = role
		reply.Index = reduceIndex
		reduceIndex++
		lock.mu.Unlock()
		return nil
	} else if role == WaitToWork {
		reply.Role = role
		lock.mu.Unlock()
		return nil
	}
	return nil
}

func checkMapWorker(fileindex int) {
	i := 0
	for i < checkTimes {
		if deliveredMapTask[fileindex] == true {
			break
		}
		time.Sleep(1 * time.Second)
		i++
	}
	role = MapWorker
	failedfiles = append(failedfiles, fileindex)
}

func checkReduceWorker(fileindex int) {
	i := 0
	for i < checkTimes {
		if deliveredReduceTask[fileindex] == true {
			break
		}
		time.Sleep(1 * time.Second)
		i++
	}
	role = ReduceWorker
	failedfiles = append(failedReduceTasks, fileindex)
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
	//fmt.Printf("master start listen!\n")
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
	// Your code here.
	filesall = files
	filesSize = len(files)
	finishedfiles = 0
	finishedReduceWorkers = 0
	deliveredMapTask = make([]bool, filesSize)
	deliveredReduceTask = make([]bool, reduceNum)
	role = MapWorker
	ret = false
	reduceNum = nReduce
	lock = &MUTEX{}
	c.server()
	return &c
}
