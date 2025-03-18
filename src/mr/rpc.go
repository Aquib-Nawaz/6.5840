package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type TaskType uint8

const (
	TASK_MAP TaskType = iota
	TASK_REDUCE
	TASK_NONE
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct{}

type GetTaskReply struct {
	Filename []string
	TaskType TaskType
	Id       int
	NReduce  int
}

type MarkTaskAsDoneArgs struct {
	Id       int
	TaskType TaskType
}

type MarkTaskAsDoneReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
