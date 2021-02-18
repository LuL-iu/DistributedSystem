package mr

import (
	"container/list"
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
	mu                sync.Mutex
	nReduce           int
	queue             list.List
	mapTaskChecker    []int
	mapFiles          []string
	mapDone           int
	reduceDone        int
	reduceTaskChecker []int
	Finished          bool
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Handler(args *Args, reply *Reply) error {
	c.mu.Lock()
	if args.MapDone == true {
		c.mapDone++
		c.mapTaskChecker[args.MapTaskNo] = -1
		c.mu.Unlock()
		return nil
	}
	if args.ReduceDone == true {
		c.reduceDone++
		c.reduceTaskChecker[args.ReduceTaskNo] = -1
		c.mu.Unlock()
		return nil
	}
	if c.mapDone < len(c.mapFiles) {
		index := -1
		for i := 0; i < len(c.mapTaskChecker); i++ {
			if c.mapTaskChecker[i] == 0 {
				c.mapTaskChecker[i] = 1
				index = i
				break
			}
		}
		if index != -1 {
			reply.MapTask = true
			reply.MapFile = c.mapFiles[index]
			reply.MapTaskNo = index
			reply.NReduce = c.nReduce
			// fmt.Printf("map %v\n", reply.MapTaskNo)
		}
		c.mu.Unlock()
		return nil
	}
	if c.reduceDone < c.nReduce {
		index := -1
		for i := 0; i < len(c.reduceTaskChecker); i++ {
			if c.reduceTaskChecker[i] == 0 {
				c.reduceTaskChecker[i] = 1
				index = i
				break
			}
		}
		if index != -1 {
			reply.ReduceTask = true
			reply.ReduceTaskNo = index
			reply.NFiles = len(c.mapFiles)
		}
		c.mu.Unlock()
		return nil
	}
	c.Finished = true
	reply.Finished = true
	c.mu.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	fmt.Printf("in server")
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := false
	for key, v := range c.mapTaskChecker {
		if v > 0 {
			c.mapTaskChecker[key]++
			// fmt.Printf("map index %v, time %v\n", key, v)
			if v == 20 {
				c.mapTaskChecker[key] = 0
			}
		}
	}
	for key, v := range c.reduceTaskChecker {
		if v > 0 {
			c.reduceTaskChecker[key]++
			// fmt.Printf("reduce index %v, time %v\n", key, v)
			if v == 20 {
				c.reduceTaskChecker[key] = 0
			}
		}
	}
	if c.Finished == true {
		ret = true
	}
	c.mu.Unlock()
	// Your code here.

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.mapFiles = files
	c.nReduce = nReduce
	for i := 0; i < len(files); i++ {
		c.mapTaskChecker = append(c.mapTaskChecker, 0)
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTaskChecker = append(c.reduceTaskChecker, 0)
	}
	c.server()
	return &c
}
