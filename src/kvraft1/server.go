package kvraft

import (
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM

	// Your definitions here.
	mu   deadlock.Mutex
	data map[string]MapValue
}

type MapValue struct {
	Value   string
	Version rpc.Tversion
}

func (kv *KVServer) InternalGet(args rpc.GetArgs) (reply rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	data, ok := kv.data[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = data.Value
		reply.Version = data.Version
		reply.Err = rpc.OK
	}
	return
}

func (kv *KVServer) InternalPut(args rpc.PutArgs) (reply rpc.PutReply) {
	//fmt.Printf("RSM:- %v InternalPut: %v\n", kv.me, args)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_data, ok := kv.data[args.Key]
	if !ok {
		if args.Version == 0 {
			kv.data[args.Key] = MapValue{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
	} else if args.Version == _data.Version {
		kv.data[args.Key] = MapValue{Value: args.Value, Version: args.Version + 1}
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrVersion
	}
	return
}

// To type-cast req to the right type, take a look at Go's type switches or type
// assertions below:
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// Your code here
	//fmt.Printf("RSM:- %v DoOp: %v\n", kv.me, req)
	switch req.(type) {
	case rpc.GetArgs:
		return kv.InternalGet(req.(rpc.GetArgs))
	case rpc.PutArgs:
		//put
		return kv.InternalPut(req.(rpc.PutArgs))
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(kv.data)
	if err != nil {
		fmt.Printf("RSM:- %v Snapshot: %v\n", kv.me, err)
		return nil
	}
	return w.Bytes()
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	err := d.Decode(&kv.data)
	if err != nil {
		fmt.Printf("RSM:- %v Restore: %v\n", kv.me, err)
	}
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a GetReply: rep.(rpc.GetReply)
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err
	if err == rpc.OK {
		*reply = rep.(rpc.GetReply)
	}

}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here. Use kv.rsm.Submit() to submit args
	// You can use go's type casts to turn the any return value
	// of Submit() into a PutReply: rep.(rpc.PutReply)
	//fmt.Printf("RSM:- %v Put: %v\n", kv.me, *args)
	err, rep := kv.rsm.Submit(*args)
	reply.Err = err
	if err == rpc.OK {
		*reply = rep.(rpc.PutReply)
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}
	kv.data = make(map[string]MapValue)
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
