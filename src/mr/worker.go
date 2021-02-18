package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	for {
		args := Args{}
		reply := Reply{}
		time.Sleep(time.Second)
		if call("Coordinator.Handler", &args, &reply) == false {
			break
		}
		fmt.Printf("in worker")
		if reply.MapTask == true {
			fmt.Printf("can do map %v %v\n", reply.MapTaskNo, reply.MapTask)
			mapFunc(reply.NReduce, reply.MapFile, reply.MapTaskNo, mapf)
			args.MapDone = true
			args.MapTaskNo = reply.MapTaskNo
			call("Coordinator.Handler", &args, &reply)
		}
		if reply.ReduceTask == true {
			fmt.Printf("can do reduce %v %v\n", reply.ReduceTaskNo, reply.ReduceTask)
			reduceFunc(reply.NFiles, reply.ReduceTaskNo, reducef)
			args.ReduceDone = true
			args.ReduceTaskNo = reply.ReduceTaskNo
			call("Coordinator.Handler", &args, &reply)
		}
		if reply.Finished == true {
			break
		}
	}

}

func mapFunc(nReduce int, filename string, mapTaskNo int, mapf func(string, string) []KeyValue) {

	files := []*os.File{}
	for i := 0; i < nReduce; i++ {
		tempF, _ := ioutil.TempFile("./", "mr-"+strconv.Itoa(mapTaskNo))
		// fmt.Printf("%v\n", tempF.Name())
		files = append(files, tempF)
	}
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
	for _, kv := range kva {
		key := kv.Key
		fileNo := ihash(key) % nReduce
		enc := json.NewEncoder(files[fileNo])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", files[fileNo].Name())
		}
	}

	for i := 0; i < nReduce; i++ {
		files[i].Close()
		os.Rename(files[i].Name(), "mr-"+strconv.Itoa(mapTaskNo)+"-"+strconv.Itoa(i))
		os.Remove(files[i].Name())
	}

}

func reduceFunc(nFiles int, reduceTaskNo int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < nFiles; i++ {
		filename := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reduceTaskNo)
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
		file.Close()
	}
	sort.Sort(ByKey(kva))

	oname := "mr-out-" + strconv.Itoa(reduceTaskNo)
	ofile, _ := os.Create(oname)

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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}
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
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
