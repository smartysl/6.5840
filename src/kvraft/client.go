package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"
import "sync"
import "sync/atomic"
import "time"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu sync.Mutex
	clientId int64
	seq int64
	leaderId int
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
	// You'll have to add code here.
	ck.clientId = nrand()

	DPrintf("New clerk initialized clientId: %v", ck.clientId)
	return ck
}

func (ck *Clerk) currentLeader() int {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	return ck.leaderId
}

func (ck *Clerk) swtichLeader() {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	DPrintf("Current leader %v isn't the real leader, switch to %v", ck.leaderId, (ck.leaderId + 1) % len(ck.servers))

	ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.

	DPrintf("Clerk %v calls Get method, key: %v", ck.clientId, key)

	args := &GetArgs{
		Key: key,
		ClientId: ck.clientId,
		Seq: atomic.AddInt64(&ck.seq, 1),
	}
	reply := &GetReply{}

	for {
		leaderId := ck.currentLeader()
		if ok := ck.servers[leaderId].Call("KVServer.Get", args, reply); ok {
			if reply.Err == OK {
				return reply.Value
			} else if reply.Err == ErrNoKey {
				return ""
			}
		}
		ck.swtichLeader()
		time.Sleep(10 * time.Millisecond)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	DPrintf("Clerk %v calls %v method, key: %v, value: %v", ck.clientId, op, key, value)

	args := &PutAppendArgs{
		Key: key,
		Value: value,
		Op: op,
		ClientId: ck.clientId,
		Seq: atomic.AddInt64(&ck.seq, 1),
	}
	reply := &PutAppendReply{}

	for {
		leaderId := ck.currentLeader()
		if ok := ck.servers[leaderId].Call("KVServer.PutAppend", args, reply); ok {
			if reply.Err == OK {
				break
			}
		}
		ck.swtichLeader()
		time.Sleep(10 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
