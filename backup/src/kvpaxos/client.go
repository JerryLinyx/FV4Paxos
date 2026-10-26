package kvpaxos

import "net/rpc"
import "fmt"
import "time"
import "math/rand"
import "sync/atomic"

type Clerk struct {
  servers []string
  // You will have to modify this struct.
  id     int64
  seq    int64
  leader int
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  ck.id = rand.Int63() ^ time.Now().UnixNano()
  ck.seq = 0
  ck.leader = 0
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  reqId := atomic.AddInt64(&ck.seq, 1)
  args := &GetArgs{Key: key, ClientId: ck.id, ReqId: reqId}
  to := 2 * time.Millisecond
  n := len(ck.servers)
  if n == 0 { return "" }
  offset := ck.leader
  for {
    window := 1
    if n < window { window = n }
    type res struct{ s int; ok bool; rep GetReply }
    ch := make(chan res, window)
    for w := 0; w < window; w++ {
      s := (offset + w) % n
      go func(idx int) {
        var reply GetReply
        ok := call(ck.servers[idx], "KVPaxos.Get", args, &reply)
        select {
        case ch <- res{s: idx, ok: ok && reply.Err == OK, rep: reply}:
        default:
        }
      }(s)
    }
    deadline := time.Now().Add(to)
    received := 0
    for time.Now().Before(deadline) && received < window {
      remaining := time.Until(deadline)
      if remaining <= 0 { break }
      select {
      case r := <-ch:
        received++
        if r.ok {
          ck.leader = r.s
          return r.rep.Value
        }
      case <-time.After(remaining):
      }
    }
    offset = (offset + window) % n
    if to < 50*time.Millisecond { to *= 2 }
  }
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  reqId := atomic.AddInt64(&ck.seq, 1)
  args := &PutArgs{Key: key, Value: value, DoHash: dohash, ClientId: ck.id, ReqId: reqId}
  to := 2 * time.Millisecond
  n := len(ck.servers)
  if n == 0 { return "" }
  offset := ck.leader
  for {
    window := 1
    if n < window { window = n }
    type res struct{ s int; ok bool; rep PutReply }
    ch := make(chan res, window)
    for w := 0; w < window; w++ {
      s := (offset + w) % n
      go func(idx int) {
        var reply PutReply
        ok := call(ck.servers[idx], "KVPaxos.Put", args, &reply)
        select {
        case ch <- res{s: idx, ok: ok && reply.Err == OK, rep: reply}:
        default:
        }
      }(s)
    }
    deadline := time.Now().Add(to)
    received := 0
    for time.Now().Before(deadline) && received < window {
      remaining := time.Until(deadline)
      if remaining <= 0 { break }
      select {
      case r := <-ch:
        received++
        if r.ok {
          ck.leader = r.s
          return r.rep.PreviousValue
        }
      case <-time.After(remaining):
      }
    }
    offset = (offset + window) % n
    if to < 50*time.Millisecond { to *= 2 }
  }
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}
