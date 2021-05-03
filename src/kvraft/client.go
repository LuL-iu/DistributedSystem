package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	serialNo  int
	curLeader int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.serialNo = 0
	ck.curLeader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	args.SerialNo = ck.serialNo
	ck.serialNo++
	reply := GetReply{}
	i := ck.curLeader
	for {
		DPrintf("[ck][Get][%v] args.key = %v , args.SerialNo = %v, curLeader = %v\n", i, args.Key, args.SerialNo, ck.curLeader)
		ck.servers[i].Call("KVServer.Get", &args, &reply)
		if reply.Err == OK {
			ck.curLeader = i
			return reply.Value
		} else if reply.Err == ErrNoKey {
			return ""
		}
		i++
		if i == len(ck.servers) {
			i = 0
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{}
	args.Key = key
	args.Op = op
	args.Value = value
	args.SerialNo = ck.serialNo
	ck.serialNo++
	reply := PutAppendReply{}
	i := ck.curLeader
	for {
		DPrintf("[ck][Get][%v] args.key = %v , args.SerialNo = %v, curLeader = %v\n", i, args.Key, args.SerialNo, ck.curLeader)
		ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		if reply.Err == OK {
			ck.curLeader = i
			return
		}
		i++
		if i == len(ck.servers) {
			i = 0
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
