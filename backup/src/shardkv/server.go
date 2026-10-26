package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	Type         string
	Key          string
	Value        string
	DoHash       bool
	ClientId     int64
	ReqId        int64
	Config       shardmaster.Config
	ConfigNum    int
	ShardData    map[int]map[string]string
	ShardClients map[int64]ClientRecord
}

type ShardStatus int

const (
	NotOwned ShardStatus = iota
	Fetching
	Serving
)

type PendingShard struct {
	ConfigNum int
	From      int64
	Servers   []string
}

type ClientRecord struct {
	Seq   int64
	Reply ClientReply
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	db           map[int]map[string]string
	config       shardmaster.Config
	shardStatus  [shardmaster.NShards]ShardStatus
	shardVersion [shardmaster.NShards]int
	pending      map[int]PendingShard
	lastApplied  int
	clientState  map[int64]ClientRecord
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	op := Op{
		Type:     "Get",
		Key:      args.Key,
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	res := kv.runOp(op)
	reply.Err = res.Reply.Err
	reply.Value = res.Reply.Value
	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	// Your code here.
	op := Op{
		Type:     "Put",
		Key:      args.Key,
		Value:    args.Value,
		DoHash:   args.DoHash,
		ClientId: args.ClientId,
		ReqId:    args.ReqId,
	}
	res := kv.runOp(op)
	reply.Err = res.Reply.Err
	reply.PreviousValue = res.Reply.Value
	return nil
}

func (kv *ShardKV) GetShard(args *GetShardArgs, reply *GetShardReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrWrongGroup
		return nil
	}
	reply.Data = make(map[int]map[string]string)
	for _, shard := range args.Shards {
		if kv.shardVersion[shard] < args.ConfigNum {
			reply.Err = ErrWrongGroup
			return nil
		}
		if data, ok := kv.db[shard]; ok {
			reply.Data[shard] = copyShard(data)
		} else {
			reply.Data[shard] = make(map[string]string)
		}
	}
	reply.ClientSeq = make(map[int64]int64)
	reply.ClientState = make(map[int64]ClientReply)
	for id, rec := range kv.clientState {
		reply.ClientSeq[id] = rec.Seq
		reply.ClientState[id] = rec.Reply
	}
	reply.Err = OK
	return nil
}

// internal helper: ensure shard map exists.
func (kv *ShardKV) ensureShard(shard int) {
	if kv.db[shard] == nil {
		kv.db[shard] = make(map[string]string)
	}
}

// internal: copy one shard map.
func copyShard(src map[string]string) map[string]string {
	dst := make(map[string]string)
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

// internal: dedup check.
func (kv *ShardKV) dupLocked(clientId int64, reqId int64) (ClientRecord, bool) {
	if clientId == 0 {
		return ClientRecord{}, false
	}
	rec, ok := kv.clientState[clientId]
	if ok && rec.Seq >= reqId {
		return rec, true
	}
	return ClientRecord{}, false
}

// internal: record client reply.
func (kv *ShardKV) recordClient(clientId int64, reqId int64, reply ClientReply) {
	if clientId == 0 {
		return
	}
	if rec, ok := kv.clientState[clientId]; !ok || rec.Seq <= reqId {
		kv.clientState[clientId] = ClientRecord{Seq: reqId, Reply: reply}
	}
}

// internal: merge client states when installing shards.
func (kv *ShardKV) mergeClientState(clients map[int64]ClientRecord) {
	for id, rec := range clients {
		cur, ok := kv.clientState[id]
		if !ok || rec.Seq > cur.Seq {
			kv.clientState[id] = rec
		}
	}
}

// internal: apply one op; kv.mu must be held.
func (kv *ShardKV) applyOp(op Op) ClientRecord {
	switch op.Type {
	case "Config":
		if op.Config.Num == kv.config.Num+1 {
			oldConfig := kv.config
			kv.config = op.Config
			DPrintf("S%v gid%v apply config %d", kv.me, kv.gid, kv.config.Num)
			for i := 0; i < shardmaster.NShards; i++ {
				delete(kv.pending, i)
				oldG := oldConfig.Shards[i]
				newG := kv.config.Shards[i]
				switch {
				case newG == kv.gid && oldG != kv.gid:
					if oldG == 0 {
						kv.ensureShard(i)
						kv.shardStatus[i] = Serving
						kv.shardVersion[i] = kv.config.Num
					} else {
						kv.shardStatus[i] = Fetching
						kv.pending[i] = PendingShard{
							ConfigNum: kv.config.Num,
							From:      oldG,
							Servers:   oldConfig.Groups[oldG],
						}
					}
				case newG == kv.gid && oldG == kv.gid:
					kv.shardStatus[i] = Serving
					kv.shardVersion[i] = kv.config.Num
				default:
					kv.shardStatus[i] = NotOwned
					kv.shardVersion[i] = kv.config.Num
				}
			}
		}
	case "InstallShard":
		if op.ConfigNum == kv.config.Num {
			for shard, data := range op.ShardData {
				if info, ok := kv.pending[shard]; ok && info.ConfigNum == op.ConfigNum {
					kv.db[shard] = copyShard(data)
					kv.shardStatus[shard] = Serving
					kv.shardVersion[shard] = op.ConfigNum
					delete(kv.pending, shard)
				}
			}
			kv.mergeClientState(op.ShardClients)
		}
	case "Put", "Get":
		if rec, ok := kv.dupLocked(op.ClientId, op.ReqId); ok {
			return rec
		}
		shard := key2shard(op.Key)
		if kv.config.Shards[shard] != kv.gid || kv.shardStatus[shard] != Serving {
			DPrintf("S%v gid%v wrong group shard %d cfg %d status %v owner %v", kv.me, kv.gid, shard, kv.config.Num, kv.shardStatus[shard], kv.config.Shards[shard])
			res := ClientReply{Err: ErrWrongGroup}
			return ClientRecord{Seq: op.ReqId, Reply: res}
		}
		kv.ensureShard(shard)
		switch op.Type {
		case "Get":
			val, ok := kv.db[shard][op.Key]
			if !ok {
				res := ClientReply{Err: ErrNoKey, Value: ""}
				kv.recordClient(op.ClientId, op.ReqId, res)
				return ClientRecord{Seq: op.ReqId, Reply: res}
			}
			res := ClientReply{Err: OK, Value: val}
			kv.recordClient(op.ClientId, op.ReqId, res)
			return ClientRecord{Seq: op.ReqId, Reply: res}
		case "Put":
			prev := kv.db[shard][op.Key]
			newVal := op.Value
			if op.DoHash {
				hv := hash(prev + op.Value)
				newVal = strconv.Itoa(int(hv))
			}
			kv.db[shard][op.Key] = newVal
			res := ClientReply{Err: OK, Value: prev}
			kv.recordClient(op.ClientId, op.ReqId, res)
			return ClientRecord{Seq: op.ReqId, Reply: res}
		}
	}
	return ClientRecord{Reply: ClientReply{Err: OK}}
}

// internal: propose and wait for decision.
func (kv *ShardKV) propose(seq int, op Op) Op {
	to := 10 * time.Millisecond
	for !kv.dead {
		decided, v := kv.px.Status(seq)
		if decided {
			if actual, ok := v.(Op); ok {
				return actual
			}
			return Op{}
		}
		kv.px.Start(seq, op)
		time.Sleep(to)
		if to < 200*time.Millisecond {
			to *= 2
		}
	}
	return Op{}
}

// run an op through Paxos and wait until it is applied.
func (kv *ShardKV) runOp(op Op) ClientRecord {
	for !kv.dead {
		kv.mu.Lock()
		if op.Type == "Get" || op.Type == "Put" {
			if rec, ok := kv.dupLocked(op.ClientId, op.ReqId); ok {
				kv.mu.Unlock()
				return rec
			}
		}
		seq := kv.lastApplied + 1
		kv.mu.Unlock()

		decidedOp := kv.propose(seq, op)

		kv.mu.Lock()
		if seq <= kv.lastApplied {
			kv.mu.Unlock()
			continue
		}
		res := kv.applyOp(decidedOp)
		kv.lastApplied = seq
		kv.px.Done(seq)
		kv.mu.Unlock()

		if decidedOp.Type == op.Type &&
			decidedOp.ClientId == op.ClientId &&
			decidedOp.ReqId == op.ReqId &&
			decidedOp.ConfigNum == op.ConfigNum &&
			decidedOp.Key == op.Key {
			return res
		}
	}
	return ClientRecord{Reply: ClientReply{Err: ErrWrongGroup}}
}

func (kv *ShardKV) pullShard(shard int, info PendingShard) {
	args := &GetShardArgs{
		ConfigNum: info.ConfigNum,
		Shards:    []int{shard},
	}
	for _, srv := range info.Servers {
		var reply GetShardReply
		ok := call(srv, "ShardKV.GetShard", args, &reply)
		if ok && reply.Err == OK {
			clients := make(map[int64]ClientRecord)
			for id, seq := range reply.ClientSeq {
				clients[id] = ClientRecord{
					Seq:   seq,
					Reply: reply.ClientState[id],
				}
			}
			kv.runOp(Op{
				Type:         "InstallShard",
				ConfigNum:    info.ConfigNum,
				ShardData:    reply.Data,
				ShardClients: clients,
			})
			return
		}
	}
}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	pending := make(map[int]PendingShard)
	for k, v := range kv.pending {
		pending[k] = v
	}
	nextNum := kv.config.Num + 1
	readyForConfig := len(kv.pending) == 0
	kv.mu.Unlock()

	if Debug > 0 {
		DPrintf("S%v gid%v tick cfg=%d pending=%d next=%d", kv.me, kv.gid, kv.config.Num, len(pending), nextNum)
	}

	if readyForConfig {
		next := kv.sm.Query(nextNum)
		if next.Num == nextNum {
			kv.runOp(Op{Type: "Config", Config: next, ConfigNum: next.Num})
		}
	}

	for shard, info := range pending {
		kv.pullShard(shard, info)
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//
//	servers that implement the shardmaster.
//
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})
	gob.Register(map[int]map[string]string{})
	gob.Register(ClientReply{})
	gob.Register(ClientRecord{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	// Don't call Join().
	kv.db = make(map[int]map[string]string)
	kv.config = kv.sm.Query(-1)
	kv.pending = make(map[int]PendingShard)
	kv.lastApplied = -1
	kv.clientState = make(map[int64]ClientRecord)
	for i := 0; i < shardmaster.NShards; i++ {
		if kv.config.Shards[i] == kv.gid {
			kv.shardStatus[i] = Serving
		} else {
			kv.shardStatus[i] = NotOwned
		}
		kv.shardVersion[i] = kv.config.Num
	}

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.dead == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
