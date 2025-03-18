package mr

import (
	"fmt"
	"log"
	"path/filepath"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskState uint8

const (
	TASK_READY TaskState = iota
	TASK_IN_PROGRESS
	TASK_DONE
)

type TaskStatus struct {
	StartTime    time.Time
	CurrentState TaskState
}

type Coordinator struct {
	mu              sync.Mutex
	files           []string
	mapTaskState    []TaskStatus
	reduceTaskState []TaskStatus
	nReduce         int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if mapId := CheckForTask(c.mapTaskState); mapId != -1 {
		reply.Filename = []string{c.files[mapId]}
		reply.TaskType = TASK_MAP
		reply.Id = mapId
		reply.NReduce = c.nReduce
		StartTask(&c.mapTaskState[mapId])
		return nil
	}
	if reduceId := CheckForTask(c.reduceTaskState); reduceId != -1 && CheckIfTaskCompleted(c.mapTaskState) {
		reply.Id = reduceId
		reply.TaskType = TASK_REDUCE
		StartTask(&c.reduceTaskState[reduceId])
		return nil
	}
	reply.TaskType = TASK_NONE
	return nil
}

func StartTask(taskStatus *TaskStatus) {
	taskStatus.StartTime = time.Now()
	taskStatus.CurrentState = TASK_IN_PROGRESS
}

func CheckForTask(state []TaskStatus) int {
	for idx, val := range state {
		if val.CurrentState == TASK_READY {
			return idx
		} else if val.CurrentState == TASK_IN_PROGRESS && time.Since(val.StartTime) > 10*time.Second {
			fmt.Printf("Task %v timed out\n", idx)
			return idx
		}
	}
	return -1
}
func CheckIfTaskCompleted(finishedTask []TaskStatus) bool {
	for _, val := range finishedTask {
		if val.CurrentState != TASK_DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) MarkTaskAsDone(args *MarkTaskAsDoneArgs, reply *struct{}) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if args.TaskType == TASK_MAP {
		c.mapTaskState[args.Id].CurrentState = TASK_DONE
	} else if args.TaskType == TASK_REDUCE {
		c.reduceTaskState[args.Id].CurrentState = TASK_DONE
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
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	ret := CheckIfTaskCompleted(c.reduceTaskState)
	c.mu.Unlock()
	if ret {
		DeleteInterMediateFiles()
	}
	return ret
}

func DeleteInterMediateFiles() {
	files, err := filepath.Glob(fmt.Sprintf("mr-out-*-*"))
	if err != nil {
		log.Fatalf("cannot read %v", err)
	}
	for _, file := range files {
		err := os.Remove(file)
		if err != nil {
			log.Fatalf("cannot delete %v", err)
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// NReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{files: files, nReduce: nReduce,
		mapTaskState: make([]TaskStatus, len(files)), reduceTaskState: make([]TaskStatus, nReduce)}

	// Your code here.
	c.server()
	return &c
}
