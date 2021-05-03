package raft

import (
	"bytes"

	"6.824/labgob"
)

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.index0)
	data := w.Bytes()
	// DPrintf("[%v][persist] term =%v, votedFor =%v", rf.me, rf.term, rf.votedFor)
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logEntries []entry
	var index0 int
	// DPrintf("[%v][readPersist] term =%v, votedFor =%v", rf.me, rf.term, rf.votedFor)
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil || d.Decode(&index0) != nil {
		//   DPrintf("Decode Error\n")
	} else {
		rf.term = term
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.index0 = index0
	}
}
