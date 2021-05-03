package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

// import "bytes"
// import "6.824/labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
// status : 0 leader, 1 candidate, 2 follower
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// curIndex	int
	//persistent state
	term       int
	votedFor   int
	logEntries []entry
	index0     int
	//
	timeCounter     int
	timeout         int
	status          int
	commitIndex     int
	lastApplied     int
	lastAppliedTerm int
	applyCh         chan ApplyMsg
	applyCond       *sync.Cond
	//leaders
	nextIndex  []int
	matchIndex []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	snapshotIndex int
	snapshotTerm  int
	snapshot      []byte

	waitSnapshotIndex int
	waitSnapshotTerm  int
	waitSnapshot      []byte
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.term = 0
	rf.timeCounter = 0
	rf.votedFor = -1
	rf.status = 2
	rf.index0 = 0
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.logEntries = make([]entry, 0)
	rf.snapshotIndex = 0
	e := entry{}
	e.Term = rf.term
	rf.logEntries = append(rf.logEntries, e)
	rf.index0 = 0

	rand.Seed(int64(rf.me))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.timeout = rand.Intn(150) + 450
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	DPrintf("[%v][Make] timeout=%v lastApplied=%v commitIndex=%v\n", rf.me, rf.timeout, rf.lastApplied, rf.commitIndex)
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyMsg()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	// Your code here (2A).
	var isleader = rf.status == 0
	term = rf.term
	return term, isleader
}

func (rf *Raft) getLastIndexTerm() (int, int) {
	if len(rf.logEntries) == 0 {
		return rf.snapshotIndex, rf.snapshotTerm
	} else {
		return rf.index0 + len(rf.logEntries) - 1, rf.logEntries[len(rf.logEntries)-1].Term
	}
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := len(rf.logEntries) + rf.index0
	term := rf.term
	isLeader := rf.status == 0
	if isLeader {
		e := entry{}
		e.Command = command
		e.Term = term
		rf.logEntries = append(rf.logEntries, e)

		rf.persist()

	}
	DPrintf("[%v][start], index : %v, isLeader : %v, term : %v, lenLog: %v\n", rf.me, index, isLeader, term, len(rf.logEntries))
	// Your code here (2B).
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) applyMsg() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.lastApplied < rf.index0 {
		rf.lastApplied = rf.index0
	}

	for !rf.killed() {
		if rf.waitSnapshot != nil {
			applyMsg := ApplyMsg{}
			applyMsg.SnapshotIndex = rf.waitSnapshotIndex
			applyMsg.SnapshotTerm = rf.waitSnapshotTerm
			applyMsg.CommandValid = false
			applyMsg.SnapshotValid = true
			applyMsg.Snapshot = rf.waitSnapshot

			rf.mu.Unlock()
			// DPrintf("[%v][AppendEntries][ApplyMsg] CommandIndex : %v, command : %v, leader : %v\n", rf.me, applyMsg.CommandIndex, applyMsg.Command, args.LeaderID)
			rf.applyCh <- applyMsg
			DPrintf("[%v][ApplyMsg][snapshot]  SnapshotIndex=%v, SnapshotTerm=%v, sizeSnapShot=%v\n", rf.me, rf.waitSnapshotIndex, rf.waitSnapshotTerm, len(applyMsg.Snapshot))
			rf.mu.Lock()
			rf.waitSnapshot = nil
		}
		if rf.lastApplied < rf.commitIndex && rf.lastApplied >= rf.index0-1 {

			rf.lastApplied++
			e := rf.logEntries[rf.lastApplied-rf.index0]
			rf.lastAppliedTerm = rf.logEntries[rf.lastApplied-rf.index0].Term
			applyMsg := ApplyMsg{}
			applyMsg.Command = e.Command
			applyMsg.CommandIndex = rf.lastApplied
			applyMsg.CommandValid = true

			rf.mu.Unlock()
			DPrintf("[%v][ApplyMsg]  commandIndex : %v\n", rf.me, applyMsg.CommandIndex)
			rf.applyCh <- applyMsg
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}

}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// fmt.Printf("counter %v, and me %v\n", rf.timeCounter, rf.me)
		rf.mu.Lock()
		if rf.status == 0 {
			go rf.askPeersAppendEntries(rf.term)
			rf.mu.Unlock()
			time.Sleep(time.Duration(100) * time.Millisecond)
		} else {
			// fmt.Printf("counter %v, and me %v\n", rf.timeCounter, rf.me)
			if rf.timeCounter > rf.timeout {
				rf.status = 1
				rf.votedFor = rf.me
				rf.term++
				DPrintf("[%v][ticker][ask for vote], term : %v\n", rf.me, rf.term)
				rf.timeCounter = 0
				rf.persist()
				go rf.askPeersForVotes(rf.term)
			}
			rf.timeCounter++
			rf.mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}
}
