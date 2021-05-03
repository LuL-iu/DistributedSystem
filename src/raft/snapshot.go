package raft

import (
	"bytes"

	"6.824/labgob"
)

type installSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
	LeaderId          int
}

type installSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshotRPC(args *installSnapshotArgs, reply *installSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if args.Term < rf.term {
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.status = 2
		rf.persist()
	}
	rf.timeCounter = 0
	rf.term = args.Term
	rf.waitSnapshotIndex = args.LastIncludedIndex
	rf.waitSnapshotTerm = args.LastIncludedTerm
	rf.waitSnapshot = args.Data
	// DPrintf("[%v][InstallSnapshotRPC] len(snapshot) = %v", rf.me, len(rf.waitSnapshot))

	DPrintf("[%v][InstallSnapshotRPC] args.lastIncludedIndex=%v args.lastIncludedTerm = %v, leaderTerm[%v] = %v, rf.term=%v, leaderID=%v\n", rf.me, args.LastIncludedIndex, args.LastIncludedTerm, args.LeaderId, args.Term, rf.term, args.LeaderId)
	rf.applyCond.Broadcast()

}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%v][CondInstallSnapshot] lastIncludedIndex=%v, rf.index0=%v， len(rf.logEntries)=%v, rf.lastApplied = %v\n", rf.me, lastIncludedIndex, rf.index0, len(rf.logEntries), rf.lastApplied)
	if lastIncludedTerm < rf.lastAppliedTerm || lastIncludedIndex <= rf.lastApplied {
		DPrintf("[%v][CondInstallSnapshot][false] lastIncludedTerm = %v, lastIncludedIndex = %v, lastApplied=%v, lastAppliedTerm=%v\n",
			rf.me, lastIncludedTerm, lastIncludedIndex, rf.lastApplied, rf.lastAppliedTerm)
		return false
	}

	rf.snapshot = snapshot
	DPrintf("[%v][CondInstallSnapshot] len(snapshot) = %v", rf.me, len(rf.snapshot))
	rf.snapshotIndex = lastIncludedIndex
	rf.snapshotTerm = lastIncludedTerm
	if lastIncludedIndex >= len(rf.logEntries)-1+rf.index0 {
		rf.logEntries = rf.logEntries[len(rf.logEntries):len(rf.logEntries)]
	} else {
		rf.logEntries = rf.logEntries[lastIncludedIndex-rf.index0+1 : len(rf.logEntries)]
	}
	rf.index0 = lastIncludedIndex + 1
	rf.lastApplied = lastIncludedIndex
	rf.lastAppliedTerm = lastIncludedTerm
	DPrintf("[%v][CondInstallSnapshot][true] lastIncludedTerm = %v, lastIncludedIndex = %v, rf.index0=%v, Size(rf.logEntries)=%v\n",
		rf.me, lastIncludedTerm, lastIncludedIndex, rf.index0, len(rf.logEntries))

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.index0)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
	// Your code here (2C).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	DPrintf("[%v][Snapshot] index = %v", rf.me, index)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2C).

	rf.snapshot = snapshot
	rf.snapshotIndex = index
	rf.snapshotTerm = rf.logEntries[index-rf.index0].Term
	rf.logEntries = rf.logEntries[index-rf.index0+1 : len(rf.logEntries)]
	rf.index0 = index + 1

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.index0)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}
