package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

import "6.5840/shardctrler"
import "log"

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongGroup  = "ErrWrongGroup"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	Seq int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	Seq int64
}

type GetReply struct {
	Err   Err
	Value string
}

type UpdateConfigArgs struct {
	Config shardctrler.Config

	ClientId int64
	Seq int64
}

type UpdateConfigReply struct {
	Err Err
}

type RequestMigrationArgs struct {
	Shards []int

	ClientId int64
	Seq int64
}

type RequestMigrationReply struct {
	Err Err

	ShardData map[int]map[string]string
}

type InsertShardArgs struct {
	ShardData map[int]map[string]string

	ClientId int64
	Seq int64
}

type InsertShardReply struct {
	Err Err
}

type NotifyMigrationSucceedArgs struct {
	Shards []int

	ClientId int64
	Seq int64
}

type NotifyMigrationSucceedReply struct {
	Err Err
}

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}