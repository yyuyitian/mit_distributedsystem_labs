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
func (c *Coordinator) ReceiveNotify(fileindex int, reply *Task) error {
	//fmt.Println("ReceiveNotify of map worker finish file: " + strconv.Itoa(fileindex))
	lock.mu.Lock()
	deliveredMapTask[fileindex] = true
	finishedfiles++
	if finishedfiles == filesSize {
		role = ReduceWorker
	}
	lock.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveReduceNotify(reduceindex int, reply *Task) error {
	//fmt.Printf("ReceiveNotify of reduce worker done!\n")
	lock.mu.Lock()
	deliveredReduceTask[reduceindex] = true
	finishedReduceWorkers++
	if finishedReduceWorkers == reduceNum {
		ret = true // whole work finished
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
			//fmt.Println("deal failed task: " + strconv.Itoa(index))
			reply.File = filesall[index]
			reply.Role = role
			reply.ReduceNum = reduceNum
			reply.Index = index
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
		reply.Index = fileIndex
		go checkMapWorker(fileIndex)
		fileIndex++
		if fileIndex == filesSize {
			role = WaitToWork
		}
		lock.mu.Unlock()
		return nil
	} else if role == ReduceWorker {
		if len(failedReduceTasks) > 0 {
			reply.Index = failedReduceTasks[len(failedReduceTasks)-1]
			//fmt.Println("deal failed reduce task: " + strconv.Itoa(reply.Index))
			reply.Role = role
			go checkReduceWorker(reply.Index)
			failedReduceTasks = failedReduceTasks[:len(failedReduceTasks)-1]
			if len(failedReduceTasks) == 0 && reduceIndex >= reduceNum {
				role = WaitToWork
			}
			lock.mu.Unlock()
			return nil
		}
		reply.Role = role
		reply.Index = reduceIndex
		go checkReduceWorker(reply.Index)
		reduceIndex++
		if reduceIndex >= reduceNum {
			role = WaitToWork
		}
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
		lock.mu.Lock()
		if deliveredMapTask[fileindex] == true {
			lock.mu.Unlock()
			break
		}
		lock.mu.Unlock()
		//fmt.Println("checkMapWorker deal with file: " + strconv.Itoa(fileindex))
		time.Sleep(2 * time.Second)
		i++
	}
	lock.mu.Lock()
	if i >= checkTimes && deliveredMapTask[fileindex] == false {
		role = MapWorker
		//fmt.Println("checkMapWorker failed, append failed files: " + strconv.Itoa(fileindex))
		failedfiles = append(failedfiles, fileindex)
		// for _, file := range failedfiles {
		// 	fmt.Println("failedfiles: " + strconv.Itoa(file))
		// }
	}
	lock.mu.Unlock()
}

func checkReduceWorker(reduceindex int) {
	i := 0
	for i < checkTimes {
		lock.mu.Lock()
		if deliveredReduceTask[reduceindex] == true {
			lock.mu.Unlock()
			break
		}
		lock.mu.Unlock()
		//fmt.Println("checkReduceWorker index: " + strconv.Itoa(reduceindex))
		time.Sleep(2 * time.Second)
		i++
	}
	lock.mu.Lock()
	if i >= checkTimes && deliveredReduceTask[reduceindex] == false {
		role = ReduceWorker
		//fmt.Println("checkReduceWorker failed, append failed: " + strconv.Itoa(reduceindex))
		failedReduceTasks = append(failedReduceTasks, reduceindex)
	}
	lock.mu.Unlock()
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
	i := 0
	for i < filesSize {
		deliveredMapTask[i] = false
		i++
	}
	role = MapWorker
	ret = false
	reduceNum = nReduce
	deliveredReduceTask = make([]bool, reduceNum)
	j := 0
	for j < reduceNum {
		deliveredReduceTask[j] = false
		j++
	}
	lock = &MUTEX{}
	c.server()
	return &c
}
