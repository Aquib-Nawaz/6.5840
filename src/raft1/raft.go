package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"fmt"
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers        []*labrpc.ClientEnd // RPC end points of all peers
	persister    *tester.Persister   // Object to hold this peer's persisted state
	me           int                 // this peer's index into peers[]
	dead         int32               // set by Kill()
	currentTerm  int
	votedFor     int
	logs         []LogEntry
	isLeader     bool
	voteReceived int
	leaderId     int
	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastCommunicationtime time.Time
	nextIndex             []int
	matchIndex            []int
	commitIndex           int
	applyCh               chan raftapi.ApplyMsg
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	return rf.currentTerm, rf.isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	debugf(rf.me, fmt.Sprintf("RequestVote received via RPC from %d in term %d args.term %d current vote for %d",
		args.CandidateId, rf.currentTerm, args.Term, rf.votedFor))

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term > rf.currentTerm {
		rf.votedFor = -1
		rf.currentTerm = args.Term
	}
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= len(rf.logs)-1) {
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
		}
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       int
	ConflictIndex int
	ConflictTerm  int
	XLen          int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	debugf(rf.me, fmt.Sprintf("AppendEntries received via RPC from %d in term %d args %v", args.LeaderId,
		rf.currentTerm, args))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.XLen = len(rf.logs)
	//failure scenario
	reply.Success = 0
	if args.Term < rf.currentTerm {
		return
	}
	reply.Term = args.Term
	rf.currentTerm = args.Term
	if rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
		debugf(rf.me, fmt.Sprintf("Chose new leader %d for term %d", rf.leaderId, rf.currentTerm))
	}
	rf.votedFor = -1
	rf.isLeader = false
	rf.lastCommunicationtime = time.Now()

	if len(rf.logs) <= args.PrevLogIndex {
		debugf(rf.me, fmt.Sprintf("AppendEntries: log too short %d %d", len(rf.logs), args.PrevLogIndex))
		return
	}
	if confTerm := rf.logs[args.PrevLogIndex].Term; confTerm != args.PrevLogTerm {
		reply.ConflictTerm = confTerm
		confIdx := args.PrevLogIndex
		for ; confIdx >= 0 && rf.logs[confIdx].Term == confTerm; confIdx-- {
		}
		reply.ConflictIndex = confIdx + 1
		debugf(rf.me, fmt.Sprintf("AppendEntries: log conflict prevLogIndex %d prevLogTerm %d conflictIndex %d conflictTerm %d",
			args.PrevLogIndex, args.PrevLogTerm, reply.ConflictIndex, reply.ConflictTerm))
		return
	}
	rf.logs = rf.logs[:args.PrevLogIndex+1]
	rf.logs = append(rf.logs, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.applyCommittedEntries(args.LeaderCommit)
	}
	reply.Success = 1
	debugf(rf.me, fmt.Sprintf("Succesfully Replicated Entries %v at index %d", args.Entries, args.PrevLogIndex+1))
}

func (rf *Raft) applyCommittedEntries(nextCommit int) {
	for idx := rf.commitIndex + 1; idx <= nextCommit; idx++ {
		rf.applyCh <- raftapi.ApplyMsg{
			CommandValid: true,
			Command:      rf.logs[idx].Command,
			CommandIndex: idx,
		}
	}
	debugf(rf.me, fmt.Sprintf("Applying Entries from %d to %d", rf.commitIndex+1, nextCommit))
	rf.commitIndex = nextCommit
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		debugf(rf.me, fmt.Sprintf("received vote %v from %d", reply.VoteGranted, server))
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1 //no longer candidate
		} else if reply.VoteGranted {
			rf.voteReceived++
			if rf.isLeader == false && 2*rf.voteReceived > len(rf.peers) {
				debugf(rf.me, "becoming leader")
				rf.isLeader = true
				for idx := range rf.peers {
					rf.nextIndex[idx] = len(rf.logs)
					rf.matchIndex[idx] = 0
				}
				rf.lastCommunicationtime = time.Now() //update last communication time
				go rf.sendAppendEntriesTicker()
			}
		}
	}
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	index := -1
	term := -1
	isLeader := rf.isLeader

	// Your code here (3B).
	if isLeader {
		rf.mu.Lock()
		index = len(rf.logs)
		rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
		term = rf.currentTerm
		rf.mu.Unlock()
	}
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := time.Duration(1000+(rand.Int63()%1500)) * time.Millisecond
		debugf(rf.me, fmt.Sprintf("Sleeping for %v s", ms))
		time.Sleep(ms)
		rf.mu.Lock()
		debugf(rf.me, fmt.Sprintf("Woke up after %v ms, last communication %v", ms.Milliseconds(), rf.lastCommunicationtime.Format(
			"15:04:05.000")))
		if !rf.isLeader && time.Now().Sub(rf.lastCommunicationtime) > ms {
			//start election
			rf.currentTerm++
			rf.votedFor = rf.me

			rf.voteReceived = 1
			rf.lastCommunicationtime = time.Now()
			rf.mu.Unlock()

			go rf.startElection()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) startElection() {
	debugf(rf.me, "Starting Election")
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendRequestVote(i, &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logs) - 1,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) sendAppendEntriesTicker() {
	for !rf.killed() && rf.isLeader {
		rf.mu.Lock()
		debugf(rf.me, "Sending Append Entries to all peers")
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				entries := rf.getEntries(i)
				go rf.sendAppendEntries(i, &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.logs[rf.nextIndex[i]-1].Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}, &AppendEntriesReply{})
			}
		}

		rf.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

func debugf(idx int, msg string) {
	if true {

		fmt.Printf("%v Peer:-%v "+msg+"\n", time.Now().Format("15:04:05.000"), idx)
	}
}
func (rf *Raft) sendAppendEntries(i int, a *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[i].Call("Raft.AppendEntries", a, reply)
	if ok {
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.isLeader = false
		} else if reply.Success == 1 {
			rf.nextIndex[i] = a.PrevLogIndex + len(a.Entries) + 1
			rf.matchIndex[i] = rf.nextIndex[i] - 1

			rf.checkForCommitIndexUpdate(rf.nextIndex[i] - 1)

		} else if reply.Success == 0 {
			rf.nextIndex[i] = rf.getNextIndexForFailure(a.PrevLogIndex, reply)
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) getEntries(i int) []LogEntry {
	if len(rf.logs) > rf.nextIndex[i] {
		return rf.logs[rf.nextIndex[i]:]
	}
	return []LogEntry{}

}

func (rf *Raft) getNextIndexForFailure(prevIndex int, reply *AppendEntriesReply) int {
	if reply.XLen <= prevIndex {
		return reply.XLen
	}
	var conflictTermIdx = -1
	for idx := prevIndex; idx >= 0 && rf.logs[idx].Term > reply.Term; idx-- {
		if rf.logs[idx].Term == reply.Term {
			conflictTermIdx = idx
			break
		}
	}
	if conflictTermIdx == -1 {
		return reply.ConflictIndex
	}
	return conflictTermIdx
}

func (rf *Raft) checkForCommitIndexUpdate(replicatedIdx int) {

	for i := replicatedIdx; i > rf.commitIndex; i-- {
		followersCommitted := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				followersCommitted++
			}
		}
		if followersCommitted > len(rf.peers)/2 {
			rf.applyCommittedEntries(i)
			break
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.logs = make([]LogEntry, 1)
	rf.logs[0].Term = 0
	rf.isLeader = false
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
