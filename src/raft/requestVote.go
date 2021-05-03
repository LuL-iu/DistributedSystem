package raft

import (
	"math/rand"
	"sync"

	"6.824/labrpc"
)

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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VotedGranted = true
	lastIndex, lastTerm := rf.getLastIndexTerm()
	DPrintf("[%v] receive request vote from %v, lastIndex = %v, lastTerm = %v, arg.lastLogTerm = %v, args.lastLogIndex = %v", rf.me, args.CandiateID, lastIndex, lastTerm, args.LastLogTerm, args.LastLogIndex)
	if rf.term > args.Term || args.LastLogTerm < lastTerm ||
		(args.LastLogTerm == lastTerm && args.LastLogIndex < lastIndex) {
		reply.VotedGranted = false
	}
	if args.Term > rf.term {
		rf.persist()
		rf.term = args.Term
		rf.status = 2
		rf.votedFor = -1
	}
	if !reply.VotedGranted {
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
}

func (rf *Raft) askVoteRPC(p *labrpc.ClientEnd, index int, curTerm int, finished *int, voters *int, cond *sync.Cond) {
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
	args.LastLogIndex, args.LastLogTerm = rf.getLastIndexTerm()
	DPrintf("[%v][askPeersForVotes], args.LastLogIndex = %v", rf.me, args.LastLogIndex)
	rf.mu.Unlock()

	p.Call("Raft.RequestVote", &args, &reply)

	rf.mu.Lock()
	*finished++
	if rf.term == curTerm {
		if reply.VotedGranted {
			*voters++
		}
	}
	cond.Broadcast()
	rf.mu.Unlock()
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
		go rf.askVoteRPC(peer, i, curTerm, &finished, &voters, cond)
	}

	rf.mu.Lock()
	for voters <= len(rf.peers)/2 && rf.status == 1 && finished < len(rf.peers) {
		// fmt.Printf("index : %v, voters: %v\n", rf.me, voters)
		cond.Wait()
	}
	if rf.status == 1 && voters > len(rf.peers)/2 {
		rf.status = 0
		for index := range rf.nextIndex {
			rf.nextIndex[index] = len(rf.logEntries) + rf.index0
		}
		for index := range rf.matchIndex {
			rf.matchIndex[index] = 0
		}
	}
	DPrintf("[%v][askPeersForVotes], voters: %v, Satus: %v, term: %v\n", rf.me, voters, rf.status, rf.term)
	rf.mu.Unlock()
}
