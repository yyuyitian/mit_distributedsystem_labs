package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"time"
)

// example to show how to declare the arguments
// and reply for an RPC.
const MapWorker int = 1
const ReduceWorker int = 3
const WaitToWork int = 0
const ExitWork int = -1

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type Task struct {
	Index     int
	Role      int
	File      string
	ReduceNum int
}

type MapResult struct {
	Kvs []KeyValue
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	//fmt.Println(s)
	return s
}

var sockid int

func workerSock() string {
	s := "/var/tmp/824-mr-"
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	sockid := r.Intn(10)
	s += strconv.Itoa(sockid)
	fmt.Println("workerSock is:" + s)
	return s
}
