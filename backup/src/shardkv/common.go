package shardkv

import "hash/fnv"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	ReqId    int64
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	ReqId    int64
}

type GetReply struct {
	Err   Err
	Value string
}

// Cached client reply used for at-most-once semantics during shard transfer.
type ClientReply struct {
	Err   Err
	Value string
}

// RPC used between groups to fetch shard data during reconfiguration.
type GetShardArgs struct {
	ConfigNum int
	Shards    []int
}

type GetShardReply struct {
	Err         Err
	Data        map[int]map[string]string
	ClientSeq   map[int64]int64
	ClientState map[int64]ClientReply
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
