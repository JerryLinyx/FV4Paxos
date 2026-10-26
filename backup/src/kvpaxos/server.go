package kvpaxos

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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     string
	Key      string
	Value    string
	DoHash   bool
	ClientId int64
	ReqId    int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db        map[string]string
	applied   int
	lastReq   map[int64]int64
	lastReply map[int64]string
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	op := Op{Type: "Get", Key: args.Key, ClientId: args.ClientId, ReqId: args.ReqId}
	val := kv.run(op)
	reply.Err = OK
	reply.Value = val
	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	op := Op{Type: "Put", Key: args.Key, Value: args.Value, DoHash: args.DoHash, ClientId: args.ClientId, ReqId: args.ReqId}
	prev := kv.run(op)
	reply.Err = OK
	reply.PreviousValue = prev
	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.db = make(map[string]string)
	kv.applied = -1
	kv.lastReq = make(map[int64]int64)
	kv.lastReply = make(map[int64]string)

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}

// internal: wait until paxos seq is decided; maxWait==0 waits indefinitely.
func (kv *KVPaxos) waitDecided(seq int, maxWait time.Duration) (bool, interface{}) {
	to := 10 * time.Millisecond
	waited := time.Duration(0)
	for !kv.dead {
		decided, v := kv.px.Status(seq)
		if decided {
			return true, v
		}
		time.Sleep(to)
		waited += to
		if maxWait > 0 && waited >= maxWait {
			return false, nil
		}
		if to < 200*time.Millisecond {
			to *= 2
		}
	}
	return false, nil
}

// internal: apply one operation; caller must hold kv.mu.
func (kv *KVPaxos) apply(op Op) string {
	if op.Type == "Noop" {
		return ""
	}
	// dedup
	if lr, ok := kv.lastReq[op.ClientId]; ok && lr >= op.ReqId {
		return kv.lastReply[op.ClientId]
	}
	switch op.Type {
	case "Put":
		prev := kv.db[op.Key]
		if op.DoHash {
			hv := hash(prev + op.Value)
			kv.db[op.Key] = fmt.Sprintf("%d", hv)
		} else {
			kv.db[op.Key] = op.Value
		}
		kv.lastReq[op.ClientId] = op.ReqId
		kv.lastReply[op.ClientId] = prev
		return prev
	case "Get":
		cur := kv.db[op.Key]
		kv.lastReq[op.ClientId] = op.ReqId
		kv.lastReply[op.ClientId] = cur
		return cur
	default:
		return ""
	}
}

// internal: check for duplicate client request; kv.mu must be held.
func (kv *KVPaxos) duplicateLocked(op Op) (string, bool) {
	if lr, ok := kv.lastReq[op.ClientId]; ok && lr >= op.ReqId {
		return kv.lastReply[op.ClientId], true
	}
	return "", false
}

// internal: drive Paxos for seq until decided; returns decided op.
func (kv *KVPaxos) proposeAndWait(seq int, op Op) (Op, bool) {
	backoff := 120 * time.Millisecond
	for !kv.dead {
		if decided, v := kv.px.Status(seq); decided {
			if actual, ok := v.(Op); ok {
				return actual, true
			}
			return Op{}, false
		}
		kv.px.Start(seq, op)
		ok, v := kv.waitDecided(seq, backoff)
		if ok {
			if actual, ok := v.(Op); ok {
				return actual, true
			}
			return Op{}, false
		}
		if backoff < time.Second {
			backoff *= 2
			if backoff > time.Second {
				backoff = time.Second
			}
		}
	}
	return Op{}, false
}

// internal: propose and ensure op is chosen and applied; return result for this op
func (kv *KVPaxos) run(op Op) string {
	for !kv.dead {
		kv.mu.Lock()
		if res, ok := kv.duplicateLocked(op); ok {
			kv.mu.Unlock()
			return res
		}
		seq := kv.applied + 1
		kv.mu.Unlock()

		decidedOp, ok := kv.proposeAndWait(seq, op)
		if !ok {
			time.Sleep(5 * time.Millisecond)
			continue
		}

		kv.mu.Lock()
		if seq <= kv.applied {
			kv.mu.Unlock()
			continue
		}
		result := kv.apply(decidedOp)
		kv.applied = seq
		kv.px.Done(seq)
		kv.mu.Unlock()

		if decidedOp.ClientId == op.ClientId && decidedOp.ReqId == op.ReqId {
			return result
		}
	}
	return ""
}
