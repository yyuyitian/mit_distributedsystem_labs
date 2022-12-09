package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
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
var serverName string

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
		fmt.Println("receive role:" + strconv.Itoa(task.Role))
		if task.Role == -1 {
			break
		}
		if task.Role == 1 {
			role = 1
			break
		} else if task.Role == 3 {
			role = 3
			break
		} else if task.Role == 0 {
			role = 0
			time.Sleep(200 * time.Millisecond)
		}
	}

	if role == 1 {
		files := task.Files
		intermediate := []KeyValue{}
		for _, filename := range files {
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
		}
		indexStr := strconv.Itoa(task.Index)
		oname := "mr-" + indexStr + "-"
		fileNameglobal = oname
		ofile, _ := os.Create(oname)

		//
		// call Reduce on each distinct key in intermediate[],
		// and print the result to mr-out-0.
		//
		i := 0
		ofilevar := ofile
		encvar := json.NewEncoder(ofilevar)

		reduceNum := task.reduceNum
		k := 0
		encvarlist := []*json.Encoder{}
		for k < reduceNum {
			oname := "reduce-" + indexStr + "-" + strconv.Itoa(k)
			ofile, _ := os.Create(oname)
			encvar := json.NewEncoder(ofile)
			encvarlist = append(encvarlist, encvar)
		}
		for i < len(intermediate) {
			word := intermediate[i].Key
			reduceIndex := ihash(word) % task.reduceNum
			encvar := encvarlist[reduceIndex]
			kv := intermediate[i]
			err := encvar.Encode(&kv)
			if err != nil {
				log.Fatalf("write failed")
			}
			i++
		}
		ofile.Close()
		notifyDone()
		// uncomment to send the Example RPC to the coordinator.
		// w := WorkerSocket{}
		// sockname := workerSock()
		// serverName = sockname
		// w.server()
		// for w.Done() == false {
		// 	time.Sleep(time.Second)
		// }
	} else if role == 3 {
		index := task.Index
		log.Println("task index is" + strconv.Itoa(index))
		var letter string
		if index == 0 {
			letter = "A"
		} else if index == 1 {
			letter = "H"
		} else if index == 2 {
			letter = "O"
		} else if index == 3 {
			letter = "U"
		}
		kva := []KeyValue{}
		v := 0
		for v <= 6 {
			filename := "mr-" + strconv.Itoa(v) + "--" + letter
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
			v += 2
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
		notifyReduceDone()
	}
}

// start a thread that listens for RPCs from worker.go
func (w *WorkerSocket) server() {
	rpc.Register(w)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	// os.Remove(sockname)
	l, e := net.Listen("unix", serverName)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Printf("listen!\n")
	go http.Serve(l, nil)
}

func (w *WorkerSocket) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// send files to reduce worker
func (w *WorkerSocket) SendfilesToReduce(index int, reply *MapResult) error {

	return nil
}

func RequestFilesFromReduce(index int, server string) []KeyValue {
	reply := MapResult{}
	fmt.Printf("RequestFilesFromReduce!\n")
	ok := callMap("WorkerSocket.SendfilesToReduce", &index, &reply, server)
	if ok {
		fmt.Printf("RequestFilesFromReduce call success!\n" + strconv.Itoa(len(reply.Kvs)))
		return reply.Kvs
	} else {
		fmt.Printf("call failed!\n")
		return nil
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

func RequstTask() *Task {
	fmt.Println("worker request task")
	args := ExampleArgs{}
	reply := Task{}
	ok := call("Coordinator.DeliverTask", &args, &reply)
	if ok {
		fmt.Println("")
	} else {
		fmt.Printf("call failed!\n")
		reply.Role = -1
	}
	return &reply
}

func notifyDone() {
	args := serverName
	reply := Task{}
	ok := call("Coordinator.ReceiveNotify", &args, &reply)
	if ok {
		fmt.Println(reply.Index)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func notifyReduceDone() {
	args := serverName
	reply := Task{}
	ok := call("Coordinator.ReceiveReduceNotify", &args, &reply)
	if ok {
		fmt.Print("call success!\n")
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
	fmt.Println("worker start dialing")
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

func callMap(rpcname string, args interface{}, reply interface{}, server string) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := server
	fmt.Printf("callMap" + sockname)
	c, err := rpc.DialHTTP("unix", sockname)
	fmt.Println("dialing")
	if err != nil {
		log.Fatal("worker start dialing:", err)
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
