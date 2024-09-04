package shardkv


import "6.5840/labrpc"
import "6.5840/raft"
import "sync"
import "6.5840/labgob"
import "6.5840/shardctrler"
import "strconv"
import "time"


const (
	OpTypeGet = "OpTypeGet"
	OpTypePut = "OpTypePut"
	OpTypeAppend = "OpTypeAppend"
	OpTypeUpdateConfig = "OpTypeUpdateConfig"
	OpTypeGetShard = "OpTypeGetShard"
	OpTypeInsertShard = "OpTypeInsertShard"
	OpTypeDeleteShard = "OpTypeDeleteShard"

	ShardStatusAvailable = "ShardStatusAvailable"
	ShardStatusMigrating = "ShardStatusMigrating"
	ShardStatusDeprecated = "ShardStatusDeprecated"
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType string
	Data interface{}
	ClientId int64
	Seq int64
}

type OpResult struct {
	op Op
	value interface{}
	err Err
}

type ShardStatus struct {
	status string
	targetGid int
}

func makeIndexTermKey(index int, term int) string {
	return strconv.Itoa(index) + "-" + strconv.Itoa(term)
}

func (kv *ShardKV) shardAvailable(shardId int) bool {
	return kv.shardStatusMap[shardId] == ShardStatusAvailable
}

func (kv *ShardKV) handleOp(op *Op) (Err, interface{}) {
	DPrintf("Group %v ShardKV instance %v handle opertion: %v", kv.gid, kv.me, op.OpType)

	if op.OpType != OpTypeGet && op.OpType != OpTypeGetShard {
		kv.mu.Lock()
		if kv.checkDuplicate(op.ClientId, op.Seq) {
			kv.mu.Unlock()
			return OK, nil
		}
		kv.mu.Unlock()
	}

	index, term, isLeader := kv.rf.Start(*op)
	if !isLeader {
		return ErrWrongLeader, nil
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

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for {
		select {
		case opResult := <- opResultChan:
			return opResult.err, opResult.value
		case <- timer.C:
			return ErrWrongLeader, nil
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	mck			 *shardctrler.Clerk
	maxraftstate int // snapshot if log grows this big

	// Your definitions here
	lastConfig *shardctrler.Config
	duringUpdateConfig bool
	shardStatusMap map[int]ShardStatus
	shardKeysMap map[int][]string
	kvMap map[string]string 
	
	opContextMap map[string]chan *OpResult
	seqMemMap map[int64]int64
}


func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := &Op{
		OpType: OpTypeGet,
		Data: args.Key,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	err, value := kv.handleOp(op)
	reply.Err = err
	if value != nil {
		reply.Value = value.(string)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	opType := OpTypePut
	if args.Op == "Append" {
		opType = OpTypeAppend
	}
	op := &Op{
		OpType: opType,
		Data: []string{args.Key, args.Value},
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	err, _ := kv.handleOp(op)
	reply.Err = err
}

// Public API
func (kv *ShardKV) RequestMigration(args *RequestMigrationArgs, reply *RequestMigrationReply) {
	op := &Op{
		OpType: OpTypeGetShard,
		Data: args.Shards,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	err, value := kv.handleOp(op)
	reply.Err = err
	if value != nil {
		reply.ShardData = value.(map[int]map[string]string)
	}
}

func (kv *ShardKV) NotifyMigrationSucceed(args *NotifyMigrationSucceedArgs, reply *NotifyMigrationSucceedReply) {
	op := &Op{
		OpType: OpTypeDeleteShard,
		Data: args.Shards,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	err, _ := kv.handleOp(op)
	reply.Err = err
}

// Internal calls
func (kv *ShardKV) UpdateConfig(args *UpdateConfigArgs) Err {
	op := &Op{
		OpType: OpTypeUpdateConfig,
		Data: args.Config,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	err, _ := kv.handleOp(op)
	return err
}

func (kv *ShardKV) InsertShard(args *InsertShardArgs) Err {
	op := &Op{
		OpType: OpTypeInsertShard,
		Data: args.ShardData,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	err, _ := kv.handleOp(op)
	return err
}

func (kv *ShardKV) sendRequestMigration(gid int, shards []int) map[int]map[string]string {
	servers := kv.lastConfig.Groups[gid]
	args := &RequestMigrationArgs{
		Shards: shards,
		ClientId: nrand(),
		Seq: 1,
	}
	for {
		for _, server := range servers {
			reply := &RequestMigrationReply{}
			srv := kv.make_end(server)
			ok := srv.Call("ShardKV.RequestMigration", args, reply)
			if ok && reply.Err == OK {
				return reply.ShardData
			}
			if reply.Err == ErrWrongGroup {
				args.Seq++
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) sendNotifyMigrationSucceed(gid int, shards []int) {
	servers := kv.lastConfig.Groups[gid]
	args := &NotifyMigrationSucceedArgs{
		Shards: shards,
		ClientId: nrand(),
		Seq: 1,
	}
	for {
		for _, server := range servers {
			reply := &NotifyMigrationSucceedReply{}
			srv := kv.make_end(server)
			ok := srv.Call("ShardKV.NotifyMigrationSucceed", args, reply)
			if ok && reply.Err == OK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (kv *ShardKV) updateConfigLoop() {
	for {
		func() {
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				return
			}

			kv.mu.Lock()
			if kv.duringUpdateConfig {
				kv.mu.Unlock()
				return
			}
			for _, status := range kv.shardStatusMap {
				if status.status != ShardStatusAvailable {
					return
				}
			}
			kv.duringUpdateConfig = true
			kv.mu.Unlock()

			newConfig := kv.mck.Query(-1)
			if kv.lastConfig != nil && newConfig.Num == kv.lastConfig.Num {
				return
			}
			var err Err
			args := &UpdateConfigArgs{
				Config: newConfig,
				ClientId: nrand(),
				Seq: 1,
			}
			for err != OK {
				err = kv.UpdateConfig(args)
			}
		}()
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) migrateLoop() {
	for {
		func() {
			_, isLeader := kv.rf.GetState()
			if !isLeader {
				return
			}

			kv.mu.Lock()
			underMigrate := make(map[int][]int)
			for shardId, status := range kv.shardStatusMap {
				if status.status == ShardStatusMigrating {
					if _, ok := underMigrate[status.targetGid]; !ok {
						underMigrate[status.targetGid] = make([]int, 0)
					}
					underMigrate[status.targetGid] = append(underMigrate[status.targetGid], shardId)
				}
			}
			kv.mu.Unlock()
			if len(underMigrate) == 0 {
				return
			}
			var wg sync.WaitGroup
			wg.Add(len(underMigrate))
	
			for gid, shards := range underMigrate {
				go func() {
					shardData := kv.sendRequestMigration(gid, shards)
					var err Err
					args := &InsertShardArgs{
						ShardData: shardData,
						ClientId: nrand(),
						Seq: 1,
					}
					for err != OK {
						err = kv.InsertShard(args)
					}
					kv.sendNotifyMigrationSucceed(gid, shards)
					wg.Done()
				}()
			}
			wg.Wait()
		}()
		time.Sleep(100 * time.Millisecond)
	}
}


// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) checkDuplicate(clientId int64, seq int64) bool {
	if prevSeq, ok := kv.seqMemMap[clientId]; ok {
		if seq <= prevSeq{
			return true
		}
	}
	return false
}

func (kv *ShardKV) applyLoop() {
	for {
		select {
		case msg := <-kv.applyCh:
			func (){
				kv.mu.Lock()
				defer kv.mu.Unlock()

				op := msg.Command.(Op)
				result := &OpResult{op: op, err: OK}
				
				switch op.OpType {
				case OpTypeGet:
					key := op.Data.(string)
					if kv.lastConfig.Shards[key2shard(key)] != kv.gid {
						result.err = ErrWrongGroup
						break
					} 
					if !kv.shardAvailable(key2shard(key)) {
						result.err = ErrWrongGroup
						break
					}
					
					if val, ok := kv.kvMap[key]; ok {
						result.value = val
					} else {
						result.err = ErrNoKey
					}
				case OpTypePut:
					DPrintf("Group %v ShardKV instance %v do put, shard status: %v", kv.gid, kv.me, kv.shardStatusMap)
					data := op.Data.([]string)
					key := data[0]
					value := data[1]
					if !kv.shardAvailable(key2shard(key)) {
						result.err = ErrWrongGroup
						break
					}

					if !kv.checkDuplicate(op.ClientId, op.Seq) {
						kv.kvMap[key] = value
					}
				case OpTypeAppend:
					data := op.Data.([]string)
					key := data[0]
					value := data[1]
					if !kv.shardAvailable(key2shard(key)) {
						result.err = ErrWrongGroup
						break
					}

					if !kv.checkDuplicate(op.ClientId, op.Seq) {
						if val, ok := kv.kvMap[key]; ok {
							kv.kvMap[key] = val + value
						} else {
							kv.kvMap[key] = value
						}
					}
				case OpTypeGetShard:
					if kv.checkDuplicate(op.ClientId, op.Seq) {
						break
					}
					shards := op.Data.([]int)
					wrongGroup := false
					for _, shardId := range shards {
						if kv.lastConfig.Shards[shardId] != kv.gid {
							wrongGroup = true
							break
						}
					}
					if wrongGroup {
						result.err = ErrWrongGroup
						break
					}
					shardData := make(map[int]map[string]string)
					for key, value := range kv.kvMap {
						for _, shardId := range shards {
							if key2shard(key) == shardId {
								if _, ok := shardData[shardId]; !ok {
									shardData[shardId] = make(map[string]string)
								}
								shardData[shardId][key] = value
							}
						}
					}
					for _, shardId := range shards {
						kv.shardStatusMap[shardId] = ShardStatus{
							status: ShardStatusDeprecated,
						}
					}
					result.value = shardData
				case OpTypeInsertShard:
					if kv.checkDuplicate(op.ClientId, op.Seq) {
						break
					}
					shardData := op.Data.(map[int]map[string]string)
					for shardId, data := range shardData {
						for key, value := range data {
							kv.kvMap[key] = value
						}
						kv.shardStatusMap[shardId] = ShardStatus{
							status: ShardStatusAvailable,
						}
					}
				case OpTypeDeleteShard:
					if kv.checkDuplicate(op.ClientId, op.Seq) {
						break
					}
					shards := op.Data.([]int)
					for key, _ := range kv.kvMap {
						for _, shardId := range shards {
							if key2shard(key) == shardId {
								delete(kv.kvMap, key)
							}
						}
					}
					for _, shardId := range shards {
						delete(kv.shardStatusMap, shardId)
					}
				case OpTypeUpdateConfig:
					if kv.checkDuplicate(op.ClientId, op.Seq) {
						break
					}
					newConfig := op.Data.(shardctrler.Config)
					if kv.lastConfig != nil {
						for shardId, gid := range newConfig.Shards {
							if gid == kv.gid && gid != kv.lastConfig.Shards[shardId] {
								kv.shardStatusMap[shardId] = ShardStatus{
									status: ShardStatusMigrating,
									targetGid: kv.lastConfig.Shards[shardId],
								}
							}
						}
						for shardId, gid := range kv.lastConfig.Shards {
							if gid == kv.gid && gid != newConfig.Shards[shardId] {
								kv.shardStatusMap[shardId] = ShardStatus{
									status: ShardStatusDeprecated,
								}
							}
						}
					} else {
						// for shardId, gid := range newConfig.Shards {
						// 	if gid == kv.gid {
						// 		kv.shardStatusMap[shardId] = ShardStatus{
						// 			status: ShardStatusMigrating,
						// 			targetGid: kv.lastConfig.Shards[shardId],
						// 		}
						// 	}
						// }
					}
					kv.lastConfig = &newConfig
					kv.duringUpdateConfig = false
				}

				kv.seqMemMap[op.ClientId] = op.Seq

				term, isLeader := kv.rf.GetState()

				if !isLeader || term != msg.CommandTerm {
					result.err = ErrWrongLeader
				}

				if ctxChan, ok := kv.opContextMap[makeIndexTermKey(msg.CommandIndex, term)]; ok {
					ctxChan <- result
				}
			}()
		}
	}
}


// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(map[int]map[string]string{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.shardStatusMap = make(map[int]string)
	kv.shardKeysMap = make(map[int][]string)
	kv.kvMap = make(map[string]string)
	kv.opContextMap = make(map[string]chan *OpResult)
	kv.seqMemMap = make(map[int64]int64)

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	go kv.updateConfigLoop()
	go kv.migrateLoop()
	go kv.applyLoop()


	return kv
}
