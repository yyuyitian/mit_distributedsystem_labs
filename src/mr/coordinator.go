package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
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
var reduceIndex int
var role int // 0 is wait; 1 is map; 2 is reduce; tell worker what should they do now
var ret bool
var lock *MUTEX

const checkTimes int = 5
const sleepTime int = 1         // 1s
var maptaskstodeliever []int    // 待分配的maptasks，每个元素代表文件的索引值，数组长度是文件的个数;其中包含处理失败的map tasks
var reducetaskstodeliever []int // 待分配的reducetasks,每个元素代表reduce worker的索引值，数组长度是reduce worker的总数;其中包含处理失败的reduce workers
var maptasksresults []bool      // 所有的maptasks的完成情况
var reducetasksresults []bool   // 所有的reducetasks的完成情况
var maptasksnum int             // 待分配的maptasks总数量
var reducetasksnum int          // 待分配的reducetasks总数量
var finishedmaptasknum int      // 已经完成的maptasks总数量
var finishedreducenum int       // 已经完成的reducetasks总数量

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) ReceiveMapNotify(fileindex int, reply *Task) error {
	//fmt.Println("ReceiveNotify of map worker finish file: " + strconv.Itoa(fileindex))
	lock.mu.Lock()
	maptasksresults[fileindex] = true
	finishedmaptasknum++
	lock.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveReduceNotify(reduceindex int, reply *Task) error {
	//fmt.Printf("ReceiveNotify of reduce worker done!\n")
	lock.mu.Lock()
	reducetasksresults[reduceindex] = true
	finishedreducenum++
	if finishedreducenum == reducetasksnum {
		ret = true // whole work finished
	}
	lock.mu.Unlock()
	return nil
}

func (c *Coordinator) DeliverTask(args *ExampleArgs, reply *Task) error {

	lock.mu.Lock()
	if finishedmaptasknum < maptasksnum {
		length := len(maptaskstodeliever)
		if length > 0 {
			reply.Role = MapWorker
			reply.Index = maptaskstodeliever[length-1]
			fmt.Println("server: Deliver map!" + strconv.Itoa(reply.Index))
			reply.File = filesall[reply.Index]
			reply.ReduceNum = reducetasksnum
			maptaskstodeliever = maptaskstodeliever[:length-1]
			fmt.Println("server: map arr length!" + strconv.Itoa(len(maptaskstodeliever)))
			go checkTask(reply.Index, &maptaskstodeliever, maptasksresults)
			lock.mu.Unlock()
			return nil
		}
		reply.Role = WaitToWork
		lock.mu.Unlock()
		return nil
	}
	if finishedmaptasknum == maptasksnum && finishedreducenum < reducetasksnum {
		length := len(reducetaskstodeliever)
		if length > 0 {
			reply.Role = ReduceWorker
			index := reducetaskstodeliever[length-1]
			reply.Index = index
			fmt.Println("server: Deliver reduce!" + strconv.Itoa(reply.Index))
			reducetaskstodeliever = reducetaskstodeliever[:length-1]
			fmt.Println("server: reduce arr length!" + strconv.Itoa(len(maptaskstodeliever)))
			go checkTask(index, &reducetaskstodeliever, reducetasksresults)
			lock.mu.Unlock()
			return nil
		}
		reply.Role = WaitToWork
		lock.mu.Unlock()
		return nil
	}
	reply.Role = WaitToWork
	lock.mu.Unlock()
	return nil
}

func checkTask(index int, taskToDeliever *[]int, taskResult []bool) {
	i := 0
	for i < checkTimes {
		lock.mu.Lock()
		if taskResult[index] == true {
			lock.mu.Unlock()
			break
		}
		lock.mu.Unlock()
		time.Sleep(2 * time.Second)
		i++
	}
	lock.mu.Lock()
	if i >= checkTimes && taskResult[index] == false {
		fmt.Println("check failed: " + strconv.Itoa(i))
		*taskToDeliever = append(*taskToDeliever, index)
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
	maptasksnum = len(filesall)
	maptaskstodeliever = make([]int, maptasksnum)
	maptasksresults = make([]bool, maptasksnum)
	i := 0
	for i < maptasksnum {
		maptaskstodeliever[i] = i
		i++
	}
	reducetasksnum = nReduce
	reducetaskstodeliever = make([]int, reducetasksnum)
	reducetasksresults = make([]bool, reducetasksnum)
	j := 0
	for j < reducetasksnum {
		reducetaskstodeliever[j] = j
		j++
	}
	ret = false
	lock = &MUTEX{}
	c.server()
	return &c
}
