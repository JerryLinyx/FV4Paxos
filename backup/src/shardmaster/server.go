package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import "sort"

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num

	lastSeq int // last applied Paxos sequence
}

type Op struct {
	Type    string
	GID     int64
	Servers []string
	Shard   int
	Num     int
	ID      int64
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	op := Op{Type: "Join", GID: args.GID, Servers: args.Servers}
	sm.runOp(op)

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	op := Op{Type: "Leave", GID: args.GID}
	sm.runOp(op)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	op := Op{Type: "Move", GID: args.GID, Shard: args.Shard}
	sm.runOp(op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	op := Op{Type: "Query", Num: args.Num}
	sm.runOp(op)

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}

	return nil
}

// runOp appends an Op to Paxos log and applies all decided entries up to it.
func (sm *ShardMaster) runOp(op Op) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op.ID = rand.Int63()
	seq := sm.px.Max() + 1
	for {
		sm.px.Start(seq, op)
		decidedOp := sm.waitForOp(seq)
		sm.applyThrough(seq)
		if decidedOp.ID == op.ID {
			break
		}
		seq++
	}
}

func (sm *ShardMaster) waitForOp(seq int) Op {
	to := 10 * time.Millisecond
	for {
		if decided, v := sm.px.Status(seq); decided {
			return v.(Op)
		}
		time.Sleep(to)
		if to < time.Second {
			to *= 2
		}
	}
}

func (sm *ShardMaster) applyThrough(seq int) {
	for sm.lastSeq < seq {
		sm.lastSeq++
		decided, v := sm.px.Status(sm.lastSeq)
		for decided == false {
			time.Sleep(10 * time.Millisecond)
			decided, v = sm.px.Status(sm.lastSeq)
		}
		op := v.(Op)
		sm.applyOp(op)
		sm.px.Done(sm.lastSeq)
	}
}

func copyConfig(c Config) Config {
	nc := Config{}
	nc.Num = c.Num
	nc.Shards = c.Shards
	nc.Groups = make(map[int64][]string)
	for gid, srv := range c.Groups {
		nc.Groups[gid] = append([]string{}, srv...)
	}
	return nc
}

func (sm *ShardMaster) applyOp(op Op) {
	if op.Type == "Query" {
		return
	}

	last := sm.configs[len(sm.configs)-1]
	nc := copyConfig(last)
	nc.Num = last.Num + 1

	switch op.Type {
	case "Join":
		if _, ok := nc.Groups[op.GID]; !ok {
			nc.Groups[op.GID] = append([]string{}, op.Servers...)
			sm.rebalance(&nc)
		}
	case "Leave":
		if _, ok := nc.Groups[op.GID]; ok {
			delete(nc.Groups, op.GID)
			sm.rebalance(&nc)
		}
	case "Move":
		nc.Shards[op.Shard] = op.GID
	}

	// Only append when config actually advances
	if op.Type == "Move" || op.Type == "Join" || op.Type == "Leave" {
		sm.configs = append(sm.configs, nc)
	}
}

func (sm *ShardMaster) rebalance(cfg *Config) {
	if len(cfg.Groups) == 0 {
		for i := 0; i < NShards; i++ {
			cfg.Shards[i] = 0
		}
		return
	}

	// current counts (including shards pointing to non-existent gid)
	counts := map[int64]int{}
	for _, gid := range cfg.Shards {
		counts[gid]++
	}

	groupsSorted := make([]int64, 0, len(cfg.Groups))
	for gid := range cfg.Groups {
		groupsSorted = append(groupsSorted, gid)
	}
	sort.Slice(groupsSorted, func(i, j int) bool { return groupsSorted[i] < groupsSorted[j] })

	// First drain shards pointing to invalid groups (including gid 0) to the least-loaded valid group.
	for i := 0; i < NShards; i++ {
		gid := cfg.Shards[i]
		if _, ok := cfg.Groups[gid]; ok {
			continue
		}
		target, _ := minCount(counts, groupsSorted)
		counts[gid]--
		cfg.Shards[i] = target
		counts[target]++
	}

	// move shards from surplus (may include gid 0 or leaving gid) to least loaded valid group
	for {
		from, fcnt := maxCount(counts)
		to, tcnt := minCount(counts, groupsSorted)

		if to == -1 || fcnt == 0 {
			break
		}

		// Balanced when no donor has more than receiver+1 and all invalid gids drained.
		_, okFromValid := cfg.Groups[from]
		if okFromValid && fcnt <= tcnt+1 {
			break
		}

		// move one shard (lowest index) from from -> to
		for i := 0; i < NShards; i++ {
			if cfg.Shards[i] == from {
				cfg.Shards[i] = to
				counts[from]--
				counts[to]++
				break
			}
		}
	}
}

func maxCount(counts map[int64]int) (int64, int) {
	var sel int64
	max := -1
	for gid, cnt := range counts {
		if cnt > max || (cnt == max && gid < sel) {
			sel = gid
			max = cnt
		}
	}
	return sel, max
}

func minCount(counts map[int64]int, valid []int64) (int64, int) {
	sel := int64(-1)
	min := -1
	for _, gid := range valid {
		cnt := counts[gid]
		if min == -1 || cnt < min || (cnt == min && gid < sel) {
			min = cnt
			sel = gid
		}
	}
	return sel, min
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}
	sm.lastSeq = -1

	rpcs := rpc.NewServer()
	rpcs.Register(sm)

	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
