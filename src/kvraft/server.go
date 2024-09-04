package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"log"
	"sync"
	"sync/atomic"
	"strconv"
	"time"
	"bytes"
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
	OpType string
	Key string
	Value string
	ClientId int64
	Seq int64
}

type OpResult struct {
	op Op
	value string
	err Err
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvMap map[string]string
	opContextMap map[string]chan *OpResult
	seqMemMap map[int64]int64
	lastApplied int
}

func makeIndexTermKey(index int, term int) string {
	return strconv.Itoa(index) + "-" + strconv.Itoa(term)
}

func (kv *KVServer) checkDuplicate(clientId int64, seq int64) bool {
	if prevSeq, ok := kv.seqMemMap[clientId]; ok {
		if seq <= prevSeq{
			return true
		}
	}
	return false
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("KVServer %v calls Get method, clientId: %v, seq: %v", kv.me, args.ClientId, args.Seq)

	op := Op{
		OpType: "Get",
		Key: args.Key,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	opResultChan := make(chan *OpResult, 1)
	kv.mu.Lock()
	kv.opContextMap[makeIndexTermKey(index, term)] = opResultChan
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if res, ok := kv.opContextMap[makeIndexTermKey(index, term)]; ok {
			if res == opResultChan {
				delete(kv.opContextMap, makeIndexTermKey(index, term))
				close(opResultChan)
			}
		}
		kv.mu.Unlock()
	}()

	defer func() {
		DPrintf("KVServer %v Get method replies, clientId: %v, seq: %v, err: %v, key: %v, value: %v", kv.me, args.ClientId, args.Seq, reply.Err, args.Key, reply.Value)
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for {
		select {
		case opResult := <- opResultChan:
			reply.Err = opResult.err
			if opResult.err == OK {
				reply.Value = opResult.value
			}
			return
		case <- timer.C:
			reply.Err = ErrWrongLeader
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("KVServer %v calls %v method, clientId: %v, seq: %v, key: %v, value: %v", kv.me, args.Op, args.ClientId, args.Seq, args.Key, args.Value)

	kv.mu.Lock()
	if kv.checkDuplicate(args.ClientId, args.Seq) {
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		OpType: args.Op,
		Key: args.Key,
		Value: args.Value,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("KVServer %v commits op to raft, index: %v, term: %v", kv.me, index, term)

	opResultChan := make(chan *OpResult, 1)
	kv.mu.Lock()
	kv.opContextMap[makeIndexTermKey(index, term)] = opResultChan
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if res, ok := kv.opContextMap[makeIndexTermKey(index, term)]; ok {
			if res == opResultChan {
				delete(kv.opContextMap, makeIndexTermKey(index, term))
				close(opResultChan)
			}
		}
		kv.mu.Unlock()
	}()

	defer func() {
		DPrintf("KVServer %v PutAppend method replies, clientId: %v, seq: %v, err: %v", kv.me, args.ClientId, args.Seq, reply.Err)
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for {
		select {
		case opResult := <- opResultChan:
			reply.Err = opResult.err
			return
		case <- timer.C:
			reply.Err = ErrWrongLeader
			return
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (kv *KVServer) applyLoop() {
	for kv.killed() == false {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				func (){
					kv.mu.Lock()
					defer kv.mu.Unlock()
	
					op := msg.Command.(Op)
					result := &OpResult{op: op}
	
					DPrintf("KVServer %v receives raft log, op type: %v, term: %v, index: %v, clientId: %v, seq: %v", kv.me, op.OpType, msg.CommandTerm, msg.CommandIndex, op.ClientId, op.Seq)
	
					if msg.CommandIndex <= kv.lastApplied {
						return
					}
	
					kv.lastApplied = msg.CommandIndex
					
					if op.OpType == "Get" {
						if val, ok := kv.kvMap[op.Key]; ok {
							result.err = OK
							result.value = val
						} else {
							result.err = ErrNoKey
						}
					} else {
						result.err = OK
						if !kv.checkDuplicate(op.ClientId, op.Seq) {
							if op.OpType == "Put" {
								kv.kvMap[op.Key] = op.Value
							} else {
								if val, ok := kv.kvMap[op.Key]; ok {
									kv.kvMap[op.Key] = val + op.Value
								} else {
									kv.kvMap[op.Key] = op.Value
								}
							}
						}
					}
					kv.seqMemMap[op.ClientId] = op.Seq
	
					term, isLeader := kv.rf.GetState()
	
					DPrintf("KVServer %v leverages op result, isLeader: %v, term: %v, index: %v, last applied: %v", kv.me, isLeader, term, msg.CommandIndex, kv.lastApplied)
	
					if !isLeader || term != msg.CommandTerm {
						result.err = ErrWrongLeader
					}
	
					if ctxChan, ok := kv.opContextMap[makeIndexTermKey(msg.CommandIndex, term)]; ok {
						ctxChan <- result
					}
				}()
			} else {
				kv.mu.Lock()
				kv.lastApplied = msg.SnapshotIndex
				kv.installSnapshot()
				DPrintf("KVServer %v receives install snapshot, snapshot index: %v", kv.me, msg.SnapshotIndex)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) snapshotLoop() {
	for kv.killed() == false {
		kv.mu.Lock()
		if kv.maxraftstate > -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
			w := new(bytes.Buffer)
			e := labgob.NewEncoder(w)
			e.Encode(kv.kvMap)
			e.Encode(kv.seqMemMap)
			kv.rf.Snapshot(kv.lastApplied, w.Bytes())
		}
		kv.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *KVServer) installSnapshot() {
	data := kv.rf.ReadSnapshot()
	if data == nil || len(data) < 1 {
		return
	}
	DPrintf("kvserver %v install the snapshot", kv.me)

	kv.kvMap = make(map[string]string)
	kv.seqMemMap = make(map[int64]int64)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&kv.kvMap)
	d.Decode(&kv.seqMemMap)
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead inG
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.kvMap = make(map[string]string)
	kv.opContextMap = make(map[string]chan *OpResult)
	kv.seqMemMap = make(map[int64]int64)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.installSnapshot()
	go kv.applyLoop()
	go kv.snapshotLoop()

	return kv
}
