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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"sort"

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
	mu          sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	curIndex	int
	//persistent state
	term        int
	votedFor    int
	logEntries  []entry
	//
	timeCounter int
	timeout     int
	status      int
	commitIndex int
	lastApplied int
	applyCh     chan ApplyMsg
	//leaders
	nextIndex  []int
	matchIndex []int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	var term int
	var isleader bool
	// Your code here (2A).
	isleader = rf.status == 0
	term = rf.term
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	// DPrintf("[%v][persist]", rf.me)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.term)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
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
	// DPrintf("[%v][readPersist] term =%v, votedFor =%v", rf.me, rf.term, rf.votedFor)
	if d.Decode(&term) != nil ||
	   d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil {
	//   DPrintf("Decode Error\n")
	} else {
      
	  rf.term = term
	  rf.votedFor = votedFor
	  rf.logEntries = logEntries
	}
	// Your code here (2C).
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

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2C).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2C).

}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandiateID   int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term         int
	VotedGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type entry struct {
	// Your data here (2A, 2B).
	Term    int
	Command interface{}
	index	int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
	XLen int
	XIndex int
	XTerm int
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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

	index := len(rf.logEntries)
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
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
			// t := time.Now()
			// fmt.Println(t.String())
			// fmt.Printf("counter %v, and me %v\n", rf.timeCounter, rf.me)
		}
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
	}
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VotedGranted = true
	if rf.term > args.Term || args.LastLogTerm < rf.logEntries[len(rf.logEntries)-1].Term ||
		(args.LastLogTerm == rf.logEntries[len(rf.logEntries)-1].Term && args.LastLogIndex < len(rf.logEntries)-1) {
		DPrintf("[%v] receive request vote from %v", rf.me, args.CandiateID)
		reply.VotedGranted = false
	}
	if args.Term > rf.term {	
		rf.persist()
		rf.term = args.Term
		rf.status = 2
		rf.votedFor = -1
	}
	if reply.VotedGranted == false {
		return
	}
	if rf.votedFor == -1 {
		rf.votedFor = args.CandiateID	
		rf.persist()
		return
	}
	if args.CandiateID == rf.votedFor {
		return 
	}
	reply.VotedGranted = false
	return
}

func (rf *Raft) askPeersForVotes(curTerm int) {
	rf.mu.Lock()
	rf.timeout = rand.Intn(150) + 450
	rf.mu.Unlock()
	voters := 1
	cond := sync.NewCond(&rf.mu)
	finished := 1
	for i, peer := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(p *labrpc.ClientEnd, index int) {
			rf.mu.Lock()
			if rf.term != curTerm {
				cond.Broadcast()
				rf.mu.Unlock()
				return
			}
			args := RequestVoteArgs{}
			reply := RequestVoteReply{}
			args.Term = rf.term
			args.CandiateID = rf.me
			args.LastLogIndex = len(rf.logEntries) - 1
			args.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
			rf.mu.Unlock()

			p.Call("Raft.RequestVote", &args, &reply)

			rf.mu.Lock()
			finished++
			if rf.term == curTerm {
				// fmt.Printf("[%v][askPeersForVote] [%v][peer] reply granted %t\n", rf.me, index, reply.VotedGranted)
				if reply.VotedGranted {
					voters++
				}
			}
			// fmt.Printf("index : %v, voters: %v\n", rf.me, voters)
			// fmt.Printf("len : %v", len(rf.peers))
			cond.Broadcast()
			rf.mu.Unlock()
		}(peer, i)
	}

	rf.mu.Lock()
	for voters <= len(rf.peers)/2 && rf.status == 1 && finished < len(rf.peers) {
		// fmt.Printf("index : %v, voters: %v\n", rf.me, voters)
		cond.Wait()
	}
	if rf.status == 1 && voters > len(rf.peers)/2 {
		rf.status = 0
		for index := range rf.nextIndex {
			rf.nextIndex[index] = len(rf.logEntries)
		}
		for index := range rf.matchIndex {
			rf.matchIndex[index] = 0
		}
	}
	DPrintf("[%v], voters: %v, Satus: %v, term: %v\n", rf.me, voters, rf.status, rf.term)
	rf.mu.Unlock()
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	DPrintf("[%v][AppendEntries][%v] args.Term=%v EntrySize = %v, prevLogIndex =%v,  preLogTerm = %v, rf.term=%v len(rf.logEntries)=%v\n", rf.me, args.LeaderID, args.Term, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, rf.term, len(rf.logEntries))

	// Your code here (2A, 2B).
	// DPrintf("[%v][AppendEntries], argTerm: %v, rfTerm: %v", rf.me, args.Term, rf.term)
	if args.Term < rf.term {
		reply.Success = false
		// reply.XLen = len(rf.logEntries)
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.status = 2
		rf.persist()
	}
	rf.timeCounter = 0

	if len(rf.logEntries) < args.PrevLogIndex+1 {
		reply.XLen = len(rf.logEntries)
		reply.Success = false
	} else if(args.PrevLogTerm != rf.logEntries[args.PrevLogIndex].Term) {
		// DPrintf("[%v][AppendEntries], len(rf.logEntries): %v, args.PrevLogTerm: %v\n", rf.me, len(rf.logEntries), args.PrevLogTerm)
		reply.XTerm = rf.logEntries[args.PrevLogIndex].Term
		x := reply.XTerm
		i := sort.Search(len(rf.logEntries), func(i int) bool { return rf.logEntries[i].Term >= x })
		if i < len(rf.logEntries) && rf.logEntries[i] .Term == x {
			reply.XIndex = i
		} else {
			reply.XIndex = args.PrevLogIndex
		}
		// reply.XLen = len(rf.logEntries)
		reply.Success = false
	} else {
		// DPrintf("[%v][AppendEntries] len(rf.logEntries)=%v args.PrevLogIndex=%v\n", rf.me, len(rf.logEntries), args.PrevLogIndex)
		if len(rf.logEntries) > args.PrevLogIndex+1 {
			rf.logEntries = rf.logEntries[:args.PrevLogIndex+1]
		}
		rf.logEntries = append(rf.logEntries, args.Entries...)
		rf.persist()
		min := Min(args.LeaderCommit, len(rf.logEntries)-1)
		if args.LeaderCommit > rf.commitIndex {
			cur := rf.commitIndex
			rf.commitIndex = min
			// DPrintf("[%v][AppendEntries][ApplyMsg]  rf.commitIndex %v, args.LeaderCommit : %v, len(rf.logEntries)-1 : %v\n", rf.me, rf.commitIndex, args.LeaderCommit, len(rf.logEntries)-1)
			for min > cur {
				cur++
				applyMsg := ApplyMsg{}
				applyMsg.Command = rf.logEntries[cur].Command
				applyMsg.CommandIndex = cur
				applyMsg.CommandValid = true
				// DPrintf("[%v][AppendEntries][ApplyMsg] CommandIndex : %v, command : %v, leader : %v\n", rf.me, applyMsg.CommandIndex, applyMsg.Command, args.LeaderID)
				rf.applyCh <- applyMsg
				rf.lastApplied = cur
			}
		}
		reply.Success = true
	}
	// DPrintf("[%v][AppendEntries], len(rf.logEntries): %v, args.PrevLogTerm: %v\n", rf.me, len(rf.logEntries), args.PrevLogTerm)
}

func (rf *Raft) askPeersAppendEntries(curTerm int) {
	finished := 1
	checked := 1
	cond := sync.NewCond(&rf.mu)

	for i, peer := range rf.peers {
		if peer == rf.peers[rf.me] {
			continue
		}
		go func(p *labrpc.ClientEnd, index int) {
			// replyV := false
			// callV := true
			// for !replyV && !callV {

			rf.mu.Lock()
			if curTerm != rf.term {
				cond.Broadcast()
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{}
			args.LeaderID = rf.me
			args.Term = rf.term
			args.Entries = rf.logEntries[rf.nextIndex[index]:len(rf.logEntries)]
			args.LeaderCommit = rf.commitIndex
			args.PrevLogIndex = rf.nextIndex[index] - 1
			args.PrevLogTerm = rf.logEntries[args.PrevLogIndex].Term
			
			reply := AppendEntriesReply{}
			reply.XLen = -1
			reply.XIndex = -1
			reply.XTerm = -1

			// DPrintf("[%v][askPeersAppendEntries][%v] rf.nextIndex = %v, Leader Term: %v\n", rf.me, index, rf.nextIndex[index], rf.term)

			rf.mu.Unlock()
			callV := p.Call("Raft.AppendEntries", &args, &reply)
			rf.mu.Lock()

			if curTerm != rf.term {
				callV = false
			}
			// DPrintf("[%v][askPeersAppendEntries] Peer: %v, return callV : %v, rf.nextIndex %v, Leader Term: %v\n", rf.me, index, callV, rf.nextIndex[index], rf.term)
			checked++
			if callV {
				finished++
				if reply.Term > rf.term {

					rf.term = reply.Term
					rf.status = 2
					rf.timeCounter = 0
					rf.persist()
				} else {
					if !reply.Success {
						if reply.XLen != -1 {
							rf.nextIndex[index] = reply.XLen
						}else {
							x := reply.XTerm
							i := sort.Search(len(rf.logEntries), func(i int) bool { return rf.logEntries[i].Term >= x })
							if i < len(rf.logEntries) && rf.logEntries[i].Term == x {
								j := sort.Search(len(rf.logEntries), func(i int) bool { return rf.logEntries[i].Term >= x + 1 }) 
								rf.nextIndex[index] = j - 1
							} else {
								rf.nextIndex[index] = reply.XIndex
							}
						}
					} else {
						if rf.nextIndex[index] == args.PrevLogIndex + 1 {
							rf.matchIndex[index] = rf.nextIndex[index] + len(args.Entries) - 1
							rf.nextIndex[index] = rf.nextIndex[index] + len(args.Entries)
						}
					}
				}
			}
			DPrintf("[%v][askPeersAppendEntries][%v] callV : %v, reply.Success=%v rf.nextIndex : %v, reply.XTerm : %v, reply.XIndex : %v, reply.XLen :%v len(args.Entries)=%v curTerm=%v\n", rf.me, index, callV, reply.Success, rf.nextIndex[index], reply.XTerm, reply.XIndex, reply.XLen, len(args.Entries), curTerm)
			
			// fmt.Printf("[%v][askPeersAppendEntries] [%v][peer] reply  %t\n", rf.me, index, reply.Success)
			cond.Broadcast()
			rf.mu.Unlock()
			// }

		}(peer, i)
	}
	rf.mu.Lock()

	for checked < len(rf.peers) && finished < len(rf.peers)/2+1 && rf.status == 0 {
		// fmt.Printf("index : %v, voters: %v\n", rf.me, voters)
		cond.Wait()
	}

	if rf.status == 0 {
		commit := rf.commitIndex + 1
		for commit < len(rf.logEntries) {
			reached := 1
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= commit {
					reached++
				}
				// DPrintf("[%v][askPeersAppendEntries][ApplyMsg] reached : %v, rf.matchIndex[%v] : %v, commit: %v\n", rf.me, reached, i, rf.matchIndex[i], commit)
			}
			if reached > len(rf.peers)/2 && rf.logEntries[commit].Term == rf.term {
				rf.commitIndex = commit
				for rf.lastApplied < commit {
					rf.lastApplied++
					e := rf.logEntries[rf.lastApplied]

					applyMsg := ApplyMsg{}
					applyMsg.Command = e.Command
					applyMsg.CommandIndex = rf.lastApplied
					applyMsg.CommandValid = true
					DPrintf("[%v][askPeersAppendEntries][ApplyMsg]  commandIndex : %v, I am leader : %v\n", rf.me, applyMsg.CommandIndex, rf.me)
					rf.applyCh <- applyMsg
				}
			}
			commit++
		}
	}

	rf.mu.Unlock()
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.term = 0
	rf.timeCounter = 0
	rf.votedFor = -1
	rf.status = 2
	rf.logEntries = make([]entry, 0)
	e := entry{}
	e.Term = rf.term
	rf.logEntries = append(rf.logEntries, e)

	rand.Seed(int64(rf.me))
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.timeout = rand.Intn(150) + 450
	// rf.persister = persister
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
