package shardctrler


import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "time"
import "strconv"
import "sort"

const(
	OpJoin = "OpJoin"
	OpLeave = "OpLeave"
	OpMove = "OpMove"
	OpQuery = "OpQuery"
)


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	opContextMap map[string]chan *OpResult
	seqMemMap map[int64]int64

	configs []Config // indexed by config num
}


type Op struct {
	// Your data here.
	OpType string
	Data interface{}
	ClientId int64
	Seq int64
}

type OpResult struct {
	op Op
	value interface{}
	wrongLeader bool
}

func makeIndexTermKey(index int, term int) string {
	return strconv.Itoa(index) + "-" + strconv.Itoa(term)
}

func (sc *ShardCtrler) handleOp(op *Op) (bool, Err, interface{}) {
	DPrintf("ShardCtrler %v handle opertion: %v", sc.me, op.OpType)

	if op.OpType != OpQuery {
		sc.mu.Lock()
		if sc.checkDuplicate(op.ClientId, op.Seq) {
			sc.mu.Unlock()
			return false, OK, nil
		}
		sc.mu.Unlock()
	}

	index, term, isLeader := sc.rf.Start(*op)
	if !isLeader {
		return true, OK, nil
	}

	opResultChan := make(chan *OpResult, 1)
	sc.mu.Lock()
	sc.opContextMap[makeIndexTermKey(index, term)] = opResultChan
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		if res, ok := sc.opContextMap[makeIndexTermKey(index, term)]; ok {
			if res == opResultChan {
				delete(sc.opContextMap, makeIndexTermKey(index, term))
				close(opResultChan)
			}
		}
		sc.mu.Unlock()
	}()

	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()

	for {
		select {
		case opResult := <- opResultChan:
			return opResult.wrongLeader, OK, opResult.value
		case <- timer.C:
			return true, OK, nil
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}


func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := &Op{
		OpType: OpJoin,
		Data: args.Servers,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	wrongLeader, err, _ := sc.handleOp(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := &Op{
		OpType: OpLeave,
		Data: args.GIDs,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	wrongLeader, err, _ := sc.handleOp(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := &Op{
		OpType: OpMove,
		Data: []int{args.Shard, args.GID},
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	wrongLeader, err, _ := sc.handleOp(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := &Op{
		OpType: OpQuery,
		Data: args.Num,
		ClientId: args.ClientId,
		Seq: args.Seq,
	}
	wrongLeader, err, data := sc.handleOp(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	if data != nil {
		reply.Config = data.(Config)
	}
}

func (sc *ShardCtrler) checkDuplicate(clientId int64, seq int64) bool {
	if prevSeq, ok := sc.seqMemMap[clientId]; ok {
		if seq <= prevSeq{
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) rebalance(conf *Config) {
	DPrintf("ShardCtrler %v rebalances, shards: %v, groups: %v", sc.me, conf.Shards, conf.Groups)

	defer func() {
		DPrintf("ShardCtrler %v after rebalanceing, shards: %v", sc.me, conf.Shards)
	}()


	gids := make([]int, 0)
	for gid, _ := range conf.Groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	if len(gids) == 0 {
		return
	}

	for {
		if conf.Shards[0] == 0 {
			for shardId, _ := range conf.Shards {
				conf.Shards[shardId] = gids[0]
			}
		}

		shardCnt := make(map[int]int)
		for _, gid := range gids {
			shardCnt[gid] = 0
		}
		for _, gid := range conf.Shards {
			shardCnt[gid]++
		}
		maxGid, maxGidCnt, minGid, minGidCnt := 0, 0, 0, 11
		for _, gid := range gids {
			cnt := shardCnt[gid]
			if cnt > maxGidCnt {
				maxGidCnt = cnt
				maxGid = gid
			}
			if cnt < minGidCnt {
				minGidCnt = cnt
				minGid = gid
			}
		}
		if maxGidCnt - minGidCnt <= 1 {
			break
		}
		for shardId, gid := range conf.Shards {
			if gid == maxGid {
				conf.Shards[shardId] = minGid
				break 
			}
		} 
	}
}

func (sc *ShardCtrler) copyConfig() *Config {
	conf := &sc.configs[len(sc.configs) - 1]
	newConfig := &Config{
		Num: conf.Num + 1,
		Shards: [NShards]int{},
		Groups: make(map[int][]string),
	}
	for shardId, gid := range conf.Shards {
		newConfig.Shards[shardId] = gid
	}
	for gid, servers := range conf.Groups {
		newConfig.Groups[gid] = servers
	}
	return newConfig
}

func (sc *ShardCtrler) applyLoop() {
	for {
		select {
		case msg := <-sc.applyCh:
			func (){
				sc.mu.Lock()
				defer sc.mu.Unlock()

				op := msg.Command.(Op)
				result := &OpResult{op: op, wrongLeader: false}
				
				switch op.OpType {
				case OpJoin:
					if !sc.checkDuplicate(op.ClientId, op.Seq) {
						config := sc.copyConfig()
						servers := op.Data.(map[int][]string)
						for gid, serverIds := range servers {
							config.Groups[gid] = serverIds
						}
						sc.rebalance(config)
						sc.configs = append(sc.configs, *config)
					}
				case OpLeave:
					if !sc.checkDuplicate(op.ClientId, op.Seq) {
						config := sc.copyConfig()
						gids := op.Data.([]int)
						for _, gid := range gids {
							delete(config.Groups, gid)
						}
						restGids := make([]int, 0)
						for gid, _ := range config.Groups {
							restGids = append(restGids, gid)
						}
						sort.Ints(restGids)
						for shardId, gid := range config.Shards {
							if _, ok := config.Groups[gid]; !ok {
								if len(restGids) == 0 {
									config.Shards[shardId] = 0
								} else {
									config.Shards[shardId] = restGids[0]
								}
							}
						}
						sc.rebalance(config)
						sc.configs = append(sc.configs, *config)
					}
				case OpMove:
					if !sc.checkDuplicate(op.ClientId, op.Seq) {
						config := sc.copyConfig()
						toMove := op.Data.([]int)
						config.Shards[toMove[0]] = toMove[1]
						sc.configs = append(sc.configs, *config)
					}
				case OpQuery:
					num := op.Data.(int)
					if num == -1 {
						num = len(sc.configs) - 1
					}
					result.value = sc.configs[num]
				}

				sc.seqMemMap[op.ClientId] = op.Seq

				term, isLeader := sc.rf.GetState()

				if !isLeader || term != msg.CommandTerm {
					result.wrongLeader = true
				}

				if ctxChan, ok := sc.opContextMap[makeIndexTermKey(msg.CommandIndex, term)]; ok {
					ctxChan <- result
				}
			}()
		}
	}
}


// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(map[int][]string{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.opContextMap = make(map[string]chan *OpResult)
	sc.seqMemMap = make(map[int64]int64)
	go sc.applyLoop()

	return sc
}
