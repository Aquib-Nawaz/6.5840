package kvraft

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"sync/atomic"
	"time"
)

const maxRetry = 3

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	leaderId int32
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// You'll have to add code here.
	return ck
}

func (ck *Clerk) atomicGetLeaderId() int32 {
	return atomic.LoadInt32(&ck.leaderId)
}

func (ck *Clerk) atomicSetLeaderId(leaderId int32) {
	atomic.StoreInt32(&ck.leaderId, leaderId)
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// You will have to modify this function.
	args := rpc.GetArgs{Key: key}
	currLeaderId := ck.atomicGetLeaderId()
	retry := 0
	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.servers[currLeaderId], "KVServer.Get", &args, &reply)
		if !ok && retry < maxRetry {
			retry++
			continue
		}
		if !ok || reply.Err == rpc.ErrWrongLeader {
			retry = 0
			currLeaderId = ck.chooseNewLeaderId(currLeaderId)
		} else {
			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC to server i with code like this:
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// You will have to modify this function.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}
	currLeaderId := ck.atomicGetLeaderId()
	retry := 0
	for {
		ok := ck.clnt.Call(ck.servers[currLeaderId], "KVServer.Put", &args, &reply)
		if !ok && retry < maxRetry {
			retry++
		} else if !ok || reply.Err == rpc.ErrWrongLeader {
			retry = 0
			currLeaderId = ck.chooseNewLeaderId(currLeaderId)
		} else {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if reply.Err == rpc.ErrVersion && retry != 0 {
		return rpc.ErrMaybe
	}
	return reply.Err
}

func (ck *Clerk) chooseNewLeaderId(currLeaderId int32) int32 {
	updatedLeaderId := ck.atomicGetLeaderId()
	if updatedLeaderId == currLeaderId {
		currLeaderId = (currLeaderId + 1) % int32(len(ck.servers))
		ck.atomicSetLeaderId(currLeaderId)
	} else {
		currLeaderId = updatedLeaderId
	}
	return currLeaderId
}
