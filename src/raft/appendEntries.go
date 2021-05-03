// example RequestVote RPC handler.
//
package raft

import (
	"sort"
	"sync"

	"6.824/labrpc"
)

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
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
	XLen    int
	XIndex  int
	XTerm   int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	DPrintf("[%v][AppendEntries][%v] curTerm = %v, args.Term=%v EntrySize = %v, prevLogIndex =%v,  preLogTerm = %v, rf.term=%v len(rf.logEntries)=%v, rf.index0=%v\n", rf.me, args.LeaderID, rf.term, args.Term, len(args.Entries), args.PrevLogIndex, args.PrevLogTerm, rf.term, len(rf.logEntries), rf.index0)

	// Your code here (2A, 2B).
	// DPrintf("[%v][AppendEntries], argTerm: %v, rfTerm: %v", rf.me, args.Term, rf.term)
	if args.Term < rf.term {
		reply.Success = false
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.status = 2
		rf.persist()
	}
	rf.timeCounter = 0

	if len(rf.logEntries)+rf.index0 < args.PrevLogIndex+1 {
		reply.XLen = len(rf.logEntries) + rf.index0
		reply.Success = false
		return
	}

	if rf.snapshotIndex == args.PrevLogIndex && args.PrevLogTerm != rf.snapshotTerm {
		reply.XTerm = rf.snapshotTerm
		reply.XIndex = rf.snapshotIndex
		reply.Success = false
		return
	}

	prevTerm := 0
	if args.PrevLogIndex == rf.index0-1 {
		prevTerm = rf.snapshotTerm
	} else if args.PrevLogIndex < rf.index0-1 {
		reply.Success = false
		return
	} else {
		prevTerm = rf.logEntries[args.PrevLogIndex-rf.index0].Term
	}

	if rf.snapshotIndex != args.PrevLogIndex && args.PrevLogTerm != prevTerm {
		// DPrintf("[%v][AppendEntries], len(rf.logEntries): %v, args.PrevLogTerm: %v\n", rf.me, len(rf.logEntries), args.PrevLogTerm)
		reply.XTerm = prevTerm
		x := reply.XTerm
		i := sort.Search(len(rf.logEntries), func(i int) bool { return rf.logEntries[i].Term >= x })
		if i < len(rf.logEntries) && rf.logEntries[i].Term == x {
			reply.XIndex = i + rf.index0
		} else {
			reply.XIndex = args.PrevLogIndex
		}
		reply.Success = false
		return
	}
	// DPrintf("[%v][AppendEntries] len(rf.logEntries)=%v args.PrevLogIndex=%v\n", rf.me, len(rf.logEntries), args.PrevLogIndex)
	if len(rf.logEntries)+rf.index0 > args.PrevLogIndex+1 {
		rf.logEntries = rf.logEntries[:args.PrevLogIndex+1-rf.index0]
	}

	rf.logEntries = append(rf.logEntries, args.Entries...)
	rf.persist()
	min := Min(args.LeaderCommit, len(rf.logEntries)-1+rf.index0)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min
		rf.applyCond.Broadcast()
		DPrintf("[%v][AppendEntries][ApplyMsg]  rf.commitIndex %v, args.LeaderCommit : %v, len(rf.logEntries)-1 : %v, curTerm: %v\n", rf.me, rf.commitIndex, args.LeaderCommit, len(rf.logEntries)-1, rf.term)
	}
	reply.Success = true
	// DPrintf("[%v][AppendEntries], len(rf.logEntries): %v, args.PrevLogTerm: %v\n", rf.me, len(rf.logEntries), args.PrevLogTerm)
}

func (rf *Raft) sendAppendEntriesRPC(p *labrpc.ClientEnd, index int, curTerm int, cond *sync.Cond, checked *int, finished *int) {
	rf.mu.Lock()
	if curTerm != rf.term {
		cond.Broadcast()
		rf.mu.Unlock()
		return
	}

	if rf.nextIndex[index] <= rf.snapshotIndex {
		snapshotArgs := installSnapshotArgs{}
		snapshotreply := installSnapshotReply{}
		snapshotArgs.Term = rf.term
		snapshotArgs.LeaderId = rf.me
		snapshotArgs.LastIncludedIndex = rf.snapshotIndex
		snapshotArgs.LastIncludedTerm = rf.snapshotTerm
		snapshotArgs.Data = rf.snapshot
		rf.mu.Unlock()

		DPrintf("[%v][askPeersAppendEntries][installSnapShot] snapshotArgs.Term = %v, snapshotArgs.LeaderId = %v, snapshotArgs.LastIncludedIndex = %v, snapshotArgs.LastIncludedTerm = %v, currentTerm=%v\n",
			rf.me, snapshotArgs.Term, snapshotArgs.LeaderId, snapshotArgs.LastIncludedIndex, snapshotArgs.LastIncludedTerm, rf.term)
		returnV := p.Call("Raft.InstallSnapshotRPC", &snapshotArgs, &snapshotreply)

		rf.mu.Lock()
		if !returnV {
			cond.Broadcast()
			rf.mu.Unlock()
			return
		}
		if snapshotreply.Term > rf.term {
			rf.term = snapshotreply.Term
			rf.status = 2
			rf.timeCounter = 0
			rf.persist()
			cond.Broadcast()
			rf.mu.Unlock()
			return
		} else {
			rf.nextIndex[index] = rf.snapshotIndex + 1
			rf.matchIndex[index] = rf.snapshotIndex
		}
	}

	args := AppendEntriesArgs{}
	args.LeaderID = rf.me
	args.Term = rf.term
	DPrintf("[%v][askPeersAppendEntries] rf.nextIndex[%v] = %v, rf.index0 = %v, rf.snapshotIndex = %v\n", rf.me, index, rf.nextIndex[index], rf.index0, rf.snapshotIndex)
	args.Entries = rf.logEntries[rf.nextIndex[index]-rf.index0 : len(rf.logEntries)]
	args.LeaderCommit = rf.commitIndex
	if rf.nextIndex[index] == rf.index0 {
		args.PrevLogIndex = rf.snapshotIndex
		args.PrevLogTerm = rf.snapshotTerm
	} else {
		args.PrevLogIndex = rf.nextIndex[index] - 1
		args.PrevLogTerm = rf.logEntries[args.PrevLogIndex-rf.index0].Term
	}

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
	*checked++
	if callV {
		*finished++
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.status = 2
			rf.timeCounter = 0
			rf.persist()
		} else {
			if !reply.Success {
				if reply.XLen != -1 {
					rf.nextIndex[index] = reply.XLen
				} else {
					x := reply.XTerm
					i := sort.Search(len(rf.logEntries), func(i int) bool { return rf.logEntries[i].Term >= x })
					if i < len(rf.logEntries) && rf.logEntries[i].Term == x {
						j := sort.Search(len(rf.logEntries), func(i int) bool { return rf.logEntries[i].Term >= x+1 })
						rf.nextIndex[index] = j - 1 + rf.index0
					} else {
						rf.nextIndex[index] = reply.XIndex
					}
				}
			} else {
				if rf.nextIndex[index] == args.PrevLogIndex+1 {
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
}

func (rf *Raft) askPeersAppendEntries(curTerm int) {
	finished := 1
	checked := 1
	cond := sync.NewCond(&rf.mu)

	for i, peer := range rf.peers {
		if peer == rf.peers[rf.me] {
			continue
		}
		go rf.sendAppendEntriesRPC(peer, i, curTerm, cond, &checked, &finished)

	}
	rf.mu.Lock()

	for checked < len(rf.peers) && finished < len(rf.peers)/2+1 && rf.status == 0 {
		cond.Wait()
	}

	if rf.status == 0 {
		commit := rf.commitIndex
		for commit+1 < len(rf.logEntries)+rf.index0 {
			commit++
			reached := 1
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				if rf.matchIndex[i] >= commit {
					reached++
				}
			}
			DPrintf("[%v][askPeersAppendEntries] rf.index0=%v, reached = %v, commit=%v, len(rf.logEntries) =%v\n", rf.me, rf.index0, reached, commit, len(rf.logEntries))
			if reached > len(rf.peers)/2 && rf.logEntries[commit-rf.index0].Term == rf.term {
				rf.commitIndex = commit
			}
		}
		rf.applyCond.Broadcast()
	}
	DPrintf("[%v][askPeersAppendEntries] rf.index0=%v, rf.commitIndex=%v\n", rf.me, rf.index0, rf.commitIndex)

	rf.mu.Unlock()
}
