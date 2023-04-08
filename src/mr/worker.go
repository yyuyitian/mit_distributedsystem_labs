package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

type WorkerSocket struct {
	// Your definitions here.

}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var filesToDeal []string
var fileNameglobal string
var encvarlist []*json.Encoder

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var role int
	var task *Task
	for true {
		task = RequstTask()
		//fmt.Println("receive role:" + strconv.Itoa(task.Role))
		if task.Role == ExitWork {
			break
		}
		if task.Role == MapWorker {
			role = MapWorker
		} else if task.Role == ReduceWorker {
			role = ReduceWorker
		} else if task.Role == WaitToWork {
			role = WaitToWork
			continue
		}
		if role == MapWorker {
			doMapWork(task, mapf)
		} else if role == ReduceWorker {
			doReduceWork(task, reducef)
		}
		time.Sleep(2 * time.Second)
		//fmt.Println("worker require task")
	}
}

func doMapWork(task *Task, mapf func(string, string) []KeyValue) {
	//fmt.Println("map worker received file: " + strconv.Itoa(task.Index))
	filename := task.File
	intermediate := []KeyValue{}
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
	intermediate = append(intermediate, kva...)
	//indexStr := strconv.Itoa(task.Index)
	reduceNum := task.ReduceNum
	k := 0
	if len(encvarlist) == 0 {
		for k < reduceNum {
			oname := "mr-reduce-" + strconv.Itoa(os.Getpid()) + "-" + strconv.Itoa(k)
			if _, err := os.Stat(oname); err == nil {
				//fmt.Printf("File exists\n")
			} else {
				//fmt.Printf("File does not exist\n")
				ofile, _ := os.Create(oname)
				encvar := json.NewEncoder(ofile)
				encvarlist = append(encvarlist, encvar)
			}
			k++
		}
	}
	i := 0
	for i < len(intermediate) {
		word := intermediate[i].Key
		reduceIndex := ihash(word) % task.ReduceNum
		encvar := encvarlist[reduceIndex]
		kv := intermediate[i]
		err := encvar.Encode(&kv)
		if err != nil {
			log.Fatalf("write failed")
		}
		i++
	}
	notifyMapDone(task.Index)
}

func doReduceWork(task *Task, reducef func(string, []string) string) {
	index := task.Index
	//log.Println("task index is" + strconv.Itoa(index))
	kva := []KeyValue{}
	files, err := filepath.Glob(fmt.Sprintf("mr-reduce-%v-%v", "*", index))
	if err != nil {
		log.Fatalf("find reduce files fail")
	}
	for _, filename := range files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
	}
	reducefile, _ := os.Create("mr-out-" + strconv.Itoa(index))
	sort.Sort(ByKey(kva))
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(reducefile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	notifyReduceDone(index)
}

func (w *WorkerSocket) Done() bool {
	ret := false
	return ret
}

func RequstTask() *Task {
	//fmt.Println("worker request task")
	args := ExampleArgs{}
	reply := Task{}
	ok := call("Coordinator.DeliverTask", &args, &reply)
	if ok {
		//fmt.Println("")
	} else {
		fmt.Printf("call failed!\n")
		reply.Role = -1
	}
	return &reply
}

func notifyMapDone(fileindex int) {
	args := fileindex
	reply := Task{}
	ok := call("Coordinator.ReceiveMapNotify", &args, &reply)
	if !ok {
		fmt.Printf("call failed!\n")
	}
}

func notifyReduceDone(reduceindex int) {
	args := reduceindex
	reply := Task{}
	ok := call("Coordinator.ReceiveReduceNotify", &args, &reply)
	if ok {
		//fmt.Print("call success!\n")
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
	fmt.Printf("call failed has error!\n")
	fmt.Println(err)
	return false
}
