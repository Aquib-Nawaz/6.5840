package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	"6.5840/labgob"
	tester "6.5840/tester1"
	"bytes"
	"fmt"
	//	"bytes"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"github.com/sasha-s/go-deadlock"
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu           deadlock.Mutex      // Lock to protect shared access to this peer's state
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
	logIdxOffset          int
	lastApplied           int
	snapshot              []byte
}

type InstallRPCArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallRPCReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallRPCArgs, reply *InstallRPCReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	if rf.currentTerm < args.Term {
		rf.createNewTerm(rf.currentTerm)
	}
	if rf.logIdxOffset > args.LastIncludedIndex {
		return
	}
	if rf.getEntryWithAbsoluteIndex(args.LastIncludedIndex).Term != args.LastIncludedTerm {
		rf.logs = make([]LogEntry, 1)
		rf.logs[0].Term = args.LastIncludedTerm
	} else {
		rf.logs = rf.logs[args.LastIncludedIndex-rf.logIdxOffset:]
	}
	rf.logIdxOffset = args.LastIncludedIndex
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	term, isLeader = rf.currentTerm, rf.isLeader
	rf.mu.Unlock()
	return
}

type EncodingInterface struct {
	CurrentTerm       int
	VotedFor          int
	Logs              []LogEntry
	LastSnapshotIndex int
	LastSnapshotTerm  int
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	if rf.snapshot != nil {
		fmt.Printf("Peer:- %v rf.persist start\n", rf.me)
	}
	toSave := EncodingInterface{CurrentTerm: rf.currentTerm,
		VotedFor: rf.votedFor, Logs: rf.logs}
	if rf.logIdxOffset != 0 {
		toSave.LastSnapshotIndex = rf.logIdxOffset
	}
	e.Encode(toSave)
	raftstate := w.Bytes()
	rf.mu.Unlock()

	rf.persister.Save(raftstate, rf.snapshot)

	rf.mu.Lock()

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var toRestore EncodingInterface
	fmt.Printf("Peer:- %v rf.persist\n", rf.me)
	if d.Decode(&toRestore) != nil {
		fmt.Println("failed to decode")
	} else {
		rf.mu.Lock()
		rf.currentTerm = toRestore.CurrentTerm
		rf.votedFor = toRestore.VotedFor
		rf.logs = toRestore.Logs
		rf.logIdxOffset = toRestore.LastSnapshotIndex
		rf.lastApplied = toRestore.LastSnapshotIndex
		rf.mu.Unlock()
		//fmt.Printf("readPersist: %v\n", toRestore)
	}

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
	fmt.Printf("Peer:- %v before Snapshot %v offset %v log length %v\n", rf.me, index, rf.logIdxOffset, len(rf.logs))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("Peer:- %v after Snapshot %v offset %v log length %v\n", rf.me, index, rf.logIdxOffset, len(rf.logs))
	rf.logs = rf.logs[index-rf.logIdxOffset:]
	prevOffset := rf.logIdxOffset
	rf.logIdxOffset = index
	rf.snapshot = snapshot
	if prevOffset != rf.logIdxOffset {
		rf.persist()
	}
	//rf.persist()
	fmt.Printf("Peer:- %v Snapshot %v offset %v log length %v\n", rf.me, index, rf.logIdxOffset, len(rf.logs))

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
		rf.createNewTerm(args.Term)
	}
	if args.Term >= rf.currentTerm && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if args.LastLogTerm > rf.logs[len(rf.logs)-1].Term || (args.LastLogTerm == rf.logs[len(rf.logs)-1].Term && args.LastLogIndex >= rf.lastLogIndex()) {
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
		}
	}
}

func (rf *Raft) lastLogIndex() int {
	return len(rf.logs) - 1 + rf.logIdxOffset
}

func (rf *Raft) createNewTerm(term int) {
	rf.votedFor = -1
	rf.currentTerm = term
	rf.isLeader = false
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
	rf.mu.Lock()
	debugf(rf.me, fmt.Sprintf("AppendEntries received via RPC from %d in term %d args %v", args.LeaderId,
		rf.currentTerm, len(args.Entries)))
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.XLen = rf.lastLogIndex() + 1
	//failure scenario
	reply.Success = 0
	if args.Term < rf.currentTerm {
		return
	}
	if rf.leaderId != args.LeaderId {
		rf.leaderId = args.LeaderId
		debugf(rf.me, fmt.Sprintf("Chose new leader %d for term %d", rf.leaderId, rf.currentTerm))
	}
	rf.createNewTerm(args.Term)
	reply.Term = rf.currentTerm
	rf.lastCommunicationtime = time.Now()
	if reply.XLen <= args.PrevLogIndex {
		debugf(rf.me, fmt.Sprintf("AppendEntries: log too short %d %d", len(rf.logs), args.PrevLogIndex))
		//fmt.Printf("Peer:- %v Term:- %v log too short %d %d\n", rf.me, rf.currentTerm, len(rf.logs), args.PrevLogIndex)
		return
	}

	if confTerm := rf.getEntryWithAbsoluteIndex(args.PrevLogIndex).Term; confTerm != args.PrevLogTerm {
		reply.ConflictTerm = confTerm
		confIdx := args.PrevLogIndex
		for ; confIdx > rf.logIdxOffset && rf.getEntryWithAbsoluteIndex(confIdx).Term == confTerm; confIdx-- {
		}
		reply.ConflictIndex = confIdx + 1
		if reply.ConflictIndex == rf.logIdxOffset {
			//fmt.Printf("Peer:- %v Term:- %v ConflictIndex:- 0, Args:- %v, Reply:- %v, Logs:- %v\n",
			//	rf.me, rf.currentTerm, args, reply, rf.logs)
		}
		debugf(rf.me, fmt.Sprintf("AppendEntries: log conflict prevLogIndex %d prevLogTerm %d conflictIndex %d conflictTerm %d",
			args.PrevLogIndex, args.PrevLogTerm, reply.ConflictIndex, reply.ConflictTerm))
		return
	}

	var prevLogLen = len(rf.logs)
	rf.logs = rf.logs[:args.PrevLogIndex+1-rf.logIdxOffset]
	rf.logs = append(rf.logs, args.Entries...)
	if args.LeaderCommit > rf.commitIndex {
		rf.applyCommittedEntries(min(args.LeaderCommit, len(rf.logs)-1+rf.logIdxOffset))
	} else if prevLogLen != len(rf.logs) || len(args.Entries) > 0 {
		rf.persist()
	}
	reply.Success = 1
	debugf(rf.me, fmt.Sprintf("Succesfully Replicated Entries %v at index %d", len(args.Entries), args.PrevLogIndex+1))
}

func (rf *Raft) getEntryWithAbsoluteIndex(index int) LogEntry {
	return rf.logs[index-rf.logIdxOffset]
}

func (rf *Raft) applyCommittedEntries(nextCommit int) {
	rf.persist()
	if nextCommit > rf.commitIndex {
		rf.commitIndex = nextCommit
	}

}

func (rf *Raft) applyCommittedEntriesTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		toApply := make([]raftapi.ApplyMsg, 0)
		if rf.snapshot != nil && rf.lastApplied < rf.logIdxOffset {
			toApply = append(toApply, raftapi.ApplyMsg{
				CommandValid:  false,
				Snapshot:      rf.snapshot,
				SnapshotValid: true,
				SnapshotIndex: rf.logIdxOffset,
				SnapshotTerm:  rf.logs[0].Term,
			})
		} else {
			for idx := rf.lastApplied + 1; idx <= rf.commitIndex; idx++ {
				toApply = append(toApply, raftapi.ApplyMsg{
					CommandValid: true,
					Command:      rf.getEntryWithAbsoluteIndex(idx).Command,
					CommandIndex: idx,
				})
			}
		}
		rf.mu.Unlock()

		for _, msg := range toApply {
			rf.applyCh <- msg
			if msg.CommandValid {
				rf.lastApplied++
			} else {
				rf.lastApplied = msg.SnapshotIndex
			}
		}
	}
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
			rf.createNewTerm(reply.Term)
		} else if reply.VoteGranted && reply.Term == rf.currentTerm {
			rf.voteReceived++
			//fmt.Printf("Peer:- %v Term:- %v Received Vote from %d\n", rf.me, rf.currentTerm, server)
			if rf.isLeader == false && 2*rf.voteReceived > len(rf.peers) {
				debugf(rf.me, "becoming leader")
				//fmt.Printf("Peer:- %v Term:- %v Became Leader\n", rf.me, rf.currentTerm)
				rf.isLeader = true
				for idx := range rf.peers {
					rf.nextIndex[idx] = len(rf.logs) + rf.logIdxOffset
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
	rf.mu.Lock()
	isLeader := rf.isLeader

	// Your code here (3B).
	if isLeader {
		index = len(rf.logs) + rf.logIdxOffset
		//fmt.Printf("Peer:- %v Appending %v at index %d\n", rf.me, command, index)
		rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
		term = rf.currentTerm
	}
	rf.mu.Unlock()
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
			rf.startElection()
		}
		rf.mu.Unlock()

	}
}

func (rf *Raft) startElection() {
	debugf(rf.me, "Starting Election")
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go rf.sendRequestVote(i, &RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.logs) - 1 + rf.logIdxOffset,
				LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
			}, &RequestVoteReply{})
		}
	}
}

func (rf *Raft) sendAppendEntriesTicker() {
	for !rf.killed() {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			break
		}
		debugf(rf.me, "Sending Append Entries to all peers")
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				entries := rf.getEntries(i)
				go rf.sendAppendEntries(i, &AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: rf.nextIndex[i] - 1,
					PrevLogTerm:  rf.getEntryWithAbsoluteIndex(rf.nextIndex[i] - 1).Term,
					Entries:      entries,
					LeaderCommit: rf.commitIndex,
				}, &AppendEntriesReply{})
			}
		}

		rf.mu.Unlock()
		time.Sleep(150 * time.Millisecond)
	}
}

func debugf(idx int, msg string) {
	if false {
		fmt.Printf("%v Peer:-%v "+msg+"\n", time.Now().Format("15:04:05.000"), idx)
	}
}
func (rf *Raft) sendAppendEntries(i int, a *AppendEntriesArgs, reply *AppendEntriesReply) {
	//fmt.Printf("Peer:- %v Term:- %v Expecting to update nextIndex for peer %v to %v\n", rf.me, rf.currentTerm, i, a.PrevLogIndex+len(a.Entries)+1)
	ok := rf.peers[i].Call("Raft.AppendEntries", a, reply)
	if ok {
		rf.mu.Lock()
		if !rf.isLeader {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			rf.createNewTerm(reply.Term)
			rf.mu.Unlock()
			return
		}
		if reply.Term < rf.currentTerm {
			rf.mu.Unlock()
			return
		}
		if reply.Success == 1 {
			debugf(rf.me, fmt.Sprintf("Updated nextIndex for peer %v from %v to %v", i, rf.nextIndex[i], a.PrevLogIndex+len(a.Entries)+1))
			rf.nextIndex[i] = a.PrevLogIndex + len(a.Entries) + 1
			rf.matchIndex[i] = rf.nextIndex[i] - 1

			rf.checkForCommitIndexUpdate(rf.nextIndex[i] - 1)

		} else {
			rf.nextIndex[i] = rf.getNextIndexForFailure(a.PrevLogIndex, reply)
		}
		//fmt.Printf("Peer:- %v Term:- %v Updated nextIndex for peer %v to %v\n", rf.me, rf.currentTerm, i, rf.nextIndex[i])

		rf.mu.Unlock()
	}
}

func (rf *Raft) getEntries(i int) []LogEntry {

	if len(rf.logs)+rf.logIdxOffset > rf.nextIndex[i] {
		return rf.logs[rf.nextIndex[i]-rf.logIdxOffset:]
	}
	return []LogEntry{}

}

func (rf *Raft) getNextIndexForFailure(prevIndex int, reply *AppendEntriesReply) int {
	if reply.XLen <= prevIndex {
		return reply.XLen
	}
	var conflictTermIdx = -1
	debugf(rf.me, fmt.Sprintf("Peer:- %v Term:- %v getNextIndexForFailure: prevIndex %v, reply %v",
		rf.me, rf.currentTerm, prevIndex, reply))
	for idx := prevIndex; idx >= rf.logIdxOffset && rf.getEntryWithAbsoluteIndex(idx).Term >= reply.Term; idx-- {
		if rf.getEntryWithAbsoluteIndex(idx).Term == reply.Term {
			conflictTermIdx = idx
			break
		}
	}
	var ret = conflictTermIdx

	if ret == -1 {
		ret = reply.ConflictIndex
	}
	if ret == 0 {
		//fmt.Printf("Peer:- %v Term:- %v getNextIndexForFailure: prevIndex %v, reply %v\n", rf.me, rf.currentTerm,
		//	prevIndex, reply)
	}
	return ret
}

func (rf *Raft) checkForCommitIndexUpdate(replicatedIdx int) {

	for i := replicatedIdx; i > rf.commitIndex && rf.getEntryWithAbsoluteIndex(i).Term == rf.currentTerm; i-- {
		followersCommitted := 1
		for j := 0; j < len(rf.peers); j++ {
			if j == rf.me {
				continue
			}
			if rf.matchIndex[j] >= i {
				followersCommitted++
			}
		}
		if 2*followersCommitted > len(rf.peers) {
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

	rf.isLeader = false
	rf.votedFor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyCh = applyCh
	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if rf.logs == nil {
		rf.logs = make([]LogEntry, 1)
	}
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommittedEntriesTicker()
	return rf
}
