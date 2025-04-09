package rsm

import (
	"6.5840/labgob"
	"bytes"
	"fmt"
	"github.com/sasha-s/go-deadlock"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/raft1"
	"6.5840/raftapi"
	"6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Me  int
	Id  uint64
	Req any
}

type RetOp struct {
	Id  uint64
	Rep any
	Me  int
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           deadlock.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	id          uint64
	mp          map[int]chan RetOp
	idxReceived int
}

type SnapEncode struct {
	Data []byte
	Idx  int
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// The RSM should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
//
// MakeRSM() must return quickly, so it should start goroutines for
// any long-running work.
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	rsm.mp = make(map[int]chan RetOp)
	rsm.id = 1
	snap := persister.ReadSnapshot()
	if snap != nil && len(snap) > 0 {
		r := bytes.NewBuffer(snap)
		d := labgob.NewDecoder(r)
		var toRestore SnapEncode
		if d.Decode(&toRestore) != nil {
			fmt.Println("failed to decode")
		}
		rsm.idxReceived = toRestore.Idx
		rsm.sm.Restore(toRestore.Data)
	}
	go rsm.readApplyCh()
	go rsm.checkForSnapshot()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit a command to Raft, and wait for it to be committed.  It
// should return ErrWrongLeader if client should find new leader and
// try again.
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here
	op := Op{
		Me:  rsm.me,
		Req: req,
	}
	//fmt.Printf("RSM:- %v Submit: %v\n", rsm.me, op)
	if _, isLeader := rsm.Raft().GetState(); isLeader {
		rsm.mu.Lock()
		op.Id = rsm.id
		rsm.id++
		rsm.mu.Unlock()
		idx, term, isLeader := rsm.Raft().Start(op)
		if isLeader {
			//fmt.Printf("RSM:- %v I am leader: %v\n", rsm.me, op)
			ch := make(chan RetOp)
			rsm.mu.Lock()
			if val, ok := rsm.mp[idx]; ok {
				close(val)
			}
			rsm.mp[idx] = ch
			rsm.mu.Unlock()
			run := true
			for run {
				select {
				case msg, ok := <-ch:
					//fmt.Printf("RSM:- %v Received response: %v Waiting for Id %v\n", rsm.me, msg, op.Id)
					if ok && msg.Id == op.Id && msg.Me == rsm.me {
						fmt.Printf("RSM:- %v Received response: %v\n", rsm.me, op.Id)
						return rpc.OK, msg.Rep
					}
					run = false
				case <-time.After(time.Millisecond * 10):
					//rsm.mu.Lock()
					//fmt.Printf("RSM:- %v Waiting for response: %v curr received %v\n", rsm.me, op.Id, rsm.idxReceived)
					if _term, _ := rsm.Raft().GetState(); _term > term {
						//fmt.Printf("RSM:- %v Timed out: %v\n", rsm.me, op.Id)
						run = false
						go func() {
							<-ch
						}()
					}
				}
			}
		}
	}
	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}

func (rsm *RSM) readApplyCh() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			op := msg.Command.(Op)
			rsm.mu.Lock()
			rep := rsm.sm.DoOp(op.Req)
			rsm.idxReceived = msg.CommandIndex
			val, ok := rsm.mp[msg.CommandIndex]
			rsm.mu.Unlock()
			if ok {
				toSend := RetOp{Id: op.Id, Rep: rep, Me: rsm.me}
				//fmt.Printf("RSM:- %v Sending response to submit routine: %v\n", rsm.me, toSend)
				val <- toSend
				close(val)
				rsm.mu.Lock()
				delete(rsm.mp, msg.CommandIndex)
				rsm.mu.Unlock()
			}
		} else if msg.SnapshotValid {
			//fmt.Printf("RSM:- %v Received snapshot: %v\n", rsm.me, msg.SnapshotIndex)
			rsm.mu.Lock()
			r := bytes.NewBuffer(msg.Snapshot)
			d := labgob.NewDecoder(r)
			var toRestore SnapEncode
			if err := d.Decode(&toRestore); err != nil {
				fmt.Printf("RSM:- %v failed to decode\n", rsm.me)
			}
			rsm.sm.Restore(toRestore.Data)
			rsm.idxReceived = msg.SnapshotIndex
			rsm.mu.Unlock()
		}
	}
	for _, ch := range rsm.mp {
		close(ch)
	}

}

func (rsm *RSM) checkForSnapshot() {
	if rsm.maxraftstate == -1 {
		return
	}
	for {
		if rsm.rf.PersistBytes() > rsm.maxraftstate*9/10 {

			rsm.mu.Lock()
			snap := rsm.sm.Snapshot()
			idx := rsm.idxReceived
			rsm.mu.Unlock()
			toSave := SnapEncode{Data: snap, Idx: idx}
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			err := e.Encode(toSave)
			if err != nil {
				fmt.Printf("RSM:- %v checkForSnapshot: %v\n", rsm.me, err)
				return
			}
			rsm.rf.Snapshot(idx, w.Bytes())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
