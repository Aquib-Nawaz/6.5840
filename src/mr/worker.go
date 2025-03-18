package mr

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		CallCoordinatorForTask(mapf, reducef)
		time.Sleep(1)
	}
}

func CallCoordinatorToMarkTaskAsDone(reply *GetTaskReply) {
	args := MarkTaskAsDoneArgs{}
	args.Id = reply.Id
	args.TaskType = reply.TaskType
	ok := call("Coordinator.MarkTaskAsDone", &args, &struct{}{})
	if !ok {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	} else {
		fmt.Printf("Task Done Marked Successfully for %v of type %v\n", reply.Id, reply.TaskType)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallCoordinatorForTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// declare an argument structure.
	args := GetTaskArgs{}

	// declare a reply structure.
	reply := GetTaskReply{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		if HandleReply(mapf, reducef, reply) {
			CallCoordinatorToMarkTaskAsDone(&reply)
		}
	} else {
		fmt.Printf("call failed!\n")
		os.Exit(0)
	}
}

func HandleReply(mapf func(string, string) []KeyValue, reducef func(string, []string) string, reply GetTaskReply) bool {
	if reply.TaskType == TASK_MAP {
		HandleMapTask(mapf, reply)
	} else if reply.TaskType == TASK_REDUCE {
		HandleReduceTask(reducef, reply)
	} else {
		return false
	}
	return true
}
func RandString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}

func HandleReduceTask(reducef func(string, []string) string, reply GetTaskReply) {
	files, err := filepath.Glob(fmt.Sprintf("mr-out-*-%d", reply.Id))
	fmt.Printf("Received %v files for Reduce Task:- %v\n", len(files), reply.Id)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}

	var kva []KeyValue
	for _, filename := range files {
		kva = readIntermediate(filename, kva)
	}
	sort.Sort(ByKey(kva))
	temponame := RandString(10)
	oname := "mr-out-" + strconv.Itoa(reply.Id)
	ofile, _ := os.Create(temponame)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-Id.
	//
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
		_, err := fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write to %v", oname)
		}
		i = j
	}
	fmt.Printf("Received %v keys for Reduce Task:- %v pid %v\n", len(kva), reply.Id, os.Getpid())

	err = ofile.Close()
	if err != nil {
		log.Fatalf("cannot close %v", oname)
	}
	err = os.Rename(temponame, oname)
	if err != nil {
		log.Fatalf("cannot rename %v to %v", temponame, oname)
	}

}

func HandleMapTask(mapf func(string, string) []KeyValue, reply GetTaskReply) {
	content, err := os.ReadFile(reply.Filename[0])
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	if err != nil {
		return
	}
	kva := mapf(reply.Filename[0], string(content))
	fmt.Printf("Received %v keys for Map Task:- %v pid %v\n", len(kva), reply.Id, os.Getpid())
	mapId := reply.Id
	nReduce := reply.NReduce

	intermediateWriters := make([]*json.Encoder, nReduce)
	intermediateFiles := make([]*os.File, nReduce)
	for _, val := range kva {
		idx := ihash(val.Key) % nReduce
		if intermediateWriters[idx] == nil {
			intermediateWriters[idx], intermediateFiles[idx] = createIntermediateWriters(mapId)
		}
		err = intermediateWriters[idx].Encode(val)
		if err != nil {
			log.Fatalf("cannot write %v-%v %v", mapId, idx, err)
		}
	}
	closeAndRenameIntermediateFiles(intermediateFiles, mapId)
}

func readIntermediate(filename string, kva []KeyValue) []KeyValue {
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
	err = file.Close()
	if err != nil {
		return nil
	}
	//err = os.Remove(filename)
	//if err != nil {
	//	log.Fatalf("cannot remove %v", filename)
	//}
	return kva
}

func closeAndRenameIntermediateFiles(intermediateFiles []*os.File, mapId int) {
	for idx, file := range intermediateFiles {
		if file == nil {
			continue
		}
		err := file.Close()
		if err != nil {
			log.Fatalf("Failed to close intermediate file: %v", err)
		}
		newFileName := fmt.Sprintf("mr-out-%d-%d", mapId, idx)
		//fmt.Printf("Renaming intermediate file %v to %v for map task:- %v\n", file.Name(), newFileName, mapId)
		err = os.Rename(file.Name(), newFileName)
	}
}

func createIntermediateWriters(mapId int) (*json.Encoder, *os.File) {
	filename := RandString(10)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create intermediate file %d %v %v", mapId, filename, err)
	}
	return json.NewEncoder(file), file
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

	fmt.Println(err)
	return false
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
