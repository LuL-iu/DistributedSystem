package kvraft

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	//0 get, 1 put, 2 append
	Type int

	Key      string
	Value    string
	SerialNo int
}

type replyCh struct {
	Err Err
	val string
	Op  Op
}

type KVServer struct {
	mu        sync.Mutex
	me        int
	rf        *raft.Raft
	applyCh   chan raft.ApplyMsg
	dead      int32 // set by Kill()
	applyCond *sync.Cond
	quit      chan int

	maxraftstate int // snapshot if log grows this big

	kvTable   map[string]string
	kvChanels map[int]chan replyCh
	completed int
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	DPrintf("[KVServer][Get][%v] args.SerialNo: %v, kv.completed: %v, args.key: %v\n", kv.me, args.SerialNo, kv.completed, args.Key)
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// if args.SerialNo < kv.completed {
	// 	if val, ok := kv.kvTable[args.Key]; ok {
	// 		reply.Err = OK
	// 		reply.Value = val
	// 	} else {
	// 		reply.Err = ErrNoKey
	// 		reply.Value = ""
	// 	}
	// 	return
	// }
	Op := Op{0, args.Key, "", args.SerialNo}
	kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(Op)
	kv.mu.Lock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("[KVServer][Get][%v] index: %v, term: %v\n", kv.me, index, term)
	replyChanel := kv.kvChanels[index]
	if replyChanel == nil {
		replyChanel = make(chan replyCh, 1) // buffered chan so that applier won't block
		kv.kvChanels[index] = replyChanel
	}
	kv.mu.Unlock()
	result := <-kv.kvChanels[index]
	DPrintf("[KVServer][Get][receiveResult][%v] args.SerialNo: %v, kv.completed: %v, args.key: %v, result.Err: %v, result.Op: %v, index: %v, term%v\n",
		kv.me, args.SerialNo, kv.completed, args.Key, result.Err, result.Op, index, term)
	kv.mu.Lock()
	if result.Op == Op {
		reply.Err = result.Err
	} else {
		reply.Err = ErrWrongLeader
	}
	reply.Value = result.val
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("[KVServer][PutAppend][%v] args.SerialNo: %v, kv.completed: %v, args.key: %v, args.Value: %v, args.Op: %v\n", kv.me, args.SerialNo, kv.completed, args.Key, args.Value, args.Op)
	// Your code here.
	// if args.SerialNo < kv.completed {
	// 	reply.Err = OK
	// 	return
	// }
	Op := Op{}
	if args.Op == "Put" {
		Op.Type = 1
	} else {
		Op.Type = 2
	}
	Op.Key = args.Key
	Op.Value = args.Value
	Op.SerialNo = args.SerialNo
	kv.mu.Unlock()
	index, term, isLeader := kv.rf.Start(Op)
	kv.mu.Lock()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	replyChanel := kv.kvChanels[index]
	if replyChanel == nil {
		replyChanel = make(chan replyCh, 1) // buffered chan so that applier won't block
		kv.kvChanels[index] = replyChanel
	}
	kv.mu.Unlock()
	DPrintf("[KVServer][PutAppend][%v] index: %v, term: %v\n", kv.me, index, term)
	result := <-kv.kvChanels[index]
	DPrintf("[KVServer][PutAppend][receiveResult][%v] args.SerialNo: %v, kv.completed: %v, args.key: %v, args.Value: %v, result.Err: %v, result.Op: %v, index: %v, term%v\n",
		kv.me, args.SerialNo, kv.completed, args.Key, args.Value, result.Err, result.Op, index, term)
	kv.mu.Lock()
	if result.Op == Op {
		reply.Err = result.Err
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) applyMsg() {
	for {
		select {
		case applyMsg := <-kv.applyCh:
			DPrintf("[kvServer][applyMsg][%v]receive apply, index: %v\n", kv.me, applyMsg.CommandIndex)
			kv.handleCommand(applyMsg)
		case <-kv.quit:
			fmt.Println("quit")
			return
		}
	}

}

func (kv *KVServer) handleCommand(applyMsg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	command := applyMsg.Command.(Op)
	DPrintf("[KVServer][applyMsg][%v]enter", kv.me)
	replyCh := replyCh{}
	replyCh.Err = OK
	replyCh.val = ""
	replyCh.Op = command
	if command.Type == 0 {
		if val, ok := kv.kvTable[command.Key]; ok {
			replyCh.val = val
		} else {
			replyCh.Err = ErrNoKey
		}
	} else if command.Type == 1 {
		kv.kvTable[command.Key] = command.Value
	} else {
		if val, ok := kv.kvTable[command.Key]; ok {
			kv.kvTable[command.Key] = val + command.Value
		} else {
			kv.kvTable[command.Key] = command.Value
		}
		replyCh.val = kv.kvTable[command.Key]
	}
	if kv.kvChanels[applyMsg.CommandIndex] == nil {
		return
	}
	DPrintf("[KVServer][applyMsg][%v]enterenter", kv.me)
	kv.mu.Unlock()
	kv.kvChanels[applyMsg.CommandIndex] <- replyCh
	kv.mu.Lock()
	DPrintf("[KVServer][applyMsg][%v] replyCh.Err: %v, replyCh.val: %v, replyCh.Op.Type: %v, replyCh.Op.serialNo: %v\n", kv.me, replyCh.Err, replyCh.val, replyCh.Op.Type, replyCh.Op.SerialNo)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	kv.quit <- 0
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvTable = make(map[string]string)
	kv.kvChanels = make(map[int]chan replyCh)
	kv.completed = -1
	kv.quit = make(chan int)
	go kv.applyMsg()
	// You may need initialization code here.

	return kv
}
