package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  // per-instance state
  instances map[int]*pxInstance

  // each peer's Done() value (highest seq it no longer needs)
  peerDone []int

  // largest sequence index observed
  maxSeq int

  // proposer round seed to generate unique proposal numbers
  propRound int64

  // track running proposer goroutines to avoid duplicates
  running map[int]bool
}

// per-instance state structure
type pxInstance struct {
  Np     int64       // highest prepare seen
  Na     int64       // highest accept seen
  Va     interface{} // value associated with Na
  Decided bool
  Value   interface{}
}

// RPC argument / reply types (exported fields for gob)
type PrepareArgs struct {
  Seq  int
  N    int64
  Done int
  From int
}

type PrepareReply struct {
  Ok   bool
  Na   int64
  Va   interface{}
  Hint int64 // highest seen Np to help proposer bump
}

type AcceptArgs struct {
  Seq  int
  N    int64
  V    interface{}
  Done int
  From int
}

type AcceptReply struct {
  Ok   bool
  Hint int64
}

type DecideArgs struct {
  Seq  int
  V    interface{}
  Done int
  From int
}

type DecideReply struct{}

// internal helpers
func (px *Paxos) getInstance(seq int) *pxInstance {
  inst, ok := px.instances[seq]
  if !ok {
    inst = &pxInstance{Np: -1, Na: -1}
    px.instances[seq] = inst
  }
  if seq > px.maxSeq {
    px.maxSeq = seq
  }
  return inst
}

func (px *Paxos) majority(n int) int {
  return (n / 2) + 1
}

func (px *Paxos) updatePeerDone(from int, done int) {
  if from < 0 || from >= len(px.peers) { return }
  if done > px.peerDone[from] {
    px.peerDone[from] = done
  }
}

func (px *Paxos) currentMin() int {
  // Min() returns min(peerDone)+1; if any peer unknown treat as -1
  min := px.peerDone[0]
  for i := 1; i < len(px.peerDone); i++ {
    if px.peerDone[i] < min {
      min = px.peerDone[i]
    }
  }
  return min + 1
}

func (px *Paxos) gcLocked() {
  min := px.currentMin()
  for seq := range px.instances {
    if seq < min {
      delete(px.instances, seq)
    }
  }
}

// generate a proposal number greater than hint using (round, me)
func (px *Paxos) nextProposalN(hint int64) int64 {
  // form = round*len(peers) + me, ensure > hint
  round := px.propRound
  n := round*int64(len(px.peers)) + int64(px.me)
  if n <= hint {
    // bump round to exceed hint
    round = (hint/int64(len(px.peers))) + 1
    n = round*int64(len(px.peers)) + int64(px.me)
  }
  px.propRound = round + 1
  return n
}

// local acceptor handlers (also used for self without RPC)
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
  px.mu.Lock()
  defer func(){ px.mu.Unlock() }()
  // update peer done from piggyback
  px.updatePeerDone(args.From, args.Done)
  // prepare handling
  inst := px.getInstance(args.Seq)
  if args.N > inst.Np {
    inst.Np = args.N
    reply.Ok = true
    reply.Na = inst.Na
    reply.Va = inst.Va
    reply.Hint = inst.Np
  } else {
    reply.Ok = false
    reply.Hint = inst.Np
  }
  // GC after updates
  px.gcLocked()
  return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
  px.mu.Lock()
  defer func(){ px.mu.Unlock() }()
  px.updatePeerDone(args.From, args.Done)
  inst := px.getInstance(args.Seq)
  if args.N >= inst.Np {
    inst.Np = args.N
    inst.Na = args.N
    inst.Va = args.V
    reply.Ok = true
    reply.Hint = inst.Np
  } else {
    reply.Ok = false
    reply.Hint = inst.Np
  }
  px.gcLocked()
  return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer func(){ px.mu.Unlock() }()
  px.updatePeerDone(args.From, args.Done)
  inst := px.getInstance(args.Seq)
  inst.Decided = true
  inst.Value = args.V
  // once decided, can drop acceptor bookkeeping if desired, but
  // leave until GC for simplicity.
  px.gcLocked()
  return nil
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  px.mu.Lock()
  // ignore if forgotten
  if seq < px.currentMin() {
    px.mu.Unlock()
    return
  }
  // if already decided, do nothing
  if inst, ok := px.instances[seq]; ok && inst.Decided {
    px.mu.Unlock()
    return
  }
  // avoid multiple proposer goroutines per seq
  if px.running == nil { px.running = make(map[int]bool) }
  if px.running[seq] {
    px.mu.Unlock()
    return
  }
  px.running[seq] = true
  px.mu.Unlock()

  go px.proposer(seq, v)
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  px.mu.Lock()
  if px.peerDone == nil { px.peerDone = make([]int, len(px.peers)) }
  if seq > px.peerDone[px.me] {
    px.peerDone[px.me] = seq
  }
  px.gcLocked()
  px.mu.Unlock()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.maxSeq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  m := px.currentMin()
  // ensure GC according to new min
  px.gcLocked()
  return m
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()
  if seq < px.currentMin() {
    return false, nil
  }
  inst, ok := px.instances[seq]
  if !ok {
    return false, nil
  }
  if inst.Decided {
    return true, inst.Value
  }
  return false, nil
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.instances = make(map[int]*pxInstance)
  px.peerDone = make([]int, len(peers))
  for i := range px.peerDone { px.peerDone[i] = -1 }
  px.maxSeq = -1
  px.propRound = 1
  px.running = make(map[int]bool)

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}

// proposer implementation
func (px *Paxos) proposer(seq int, v interface{}) {
  // retry loop until decided or dead
  var decided bool
  var decidedVal interface{}
  for !decided && !px.dead {
    // if someone else decided, observe and stop
    if d, val := px.Status(seq); d {
      decided = true
      decidedVal = val
      break
    }
    // step 1: prepare
    px.mu.Lock()
    n := px.nextProposalN(-1)
    myDone := px.peerDone[px.me]
    peers := append([]string(nil), px.peers...)
    me := px.me
    px.mu.Unlock()

    countOK := 0
    highestNa := int64(-1)
    var chosenVa interface{}
    // contact self first for faster majority, then others; early-stop on majority
    order := make([]int, 0, len(peers))
    order = append(order, me)
    for i := 0; i < len(peers); i++ { if i != me { order = append(order, i) } }
    for _, i := range order {
      args := &PrepareArgs{Seq: seq, N: n, Done: myDone, From: me}
      var reply PrepareReply
      ok := false
      if i == me {
        px.Prepare(args, &reply)
        ok = true
      } else {
        ok = call(peers[i], "Paxos.Prepare", args, &reply)
      }
      if ok && reply.Ok {
        countOK++
        if reply.Na > highestNa {
          highestNa = reply.Na
          chosenVa = reply.Va
        }
        if countOK >= px.majority(len(peers)) { break }
      } else if ok {
        px.mu.Lock()
        if reply.Hint > n { _ = px.nextProposalN(reply.Hint) }
        px.mu.Unlock()
      }
    }

    if countOK >= px.majority(len(peers)) {
      // choose value
      useV := v
      if highestNa != -1 {
        useV = chosenVa
      }
      // step 2: accept
      accOK := 0
      for _, i := range order {
        args := &AcceptArgs{Seq: seq, N: n, V: useV, Done: myDone, From: me}
        var reply AcceptReply
        ok := false
        if i == me {
          px.Accept(args, &reply)
          ok = true
        } else {
          ok = call(peers[i], "Paxos.Accept", args, &reply)
        }
        if ok && reply.Ok {
          accOK++
          if accOK >= px.majority(len(peers)) { break }
        } else if ok {
          px.mu.Lock()
          if reply.Hint > n { _ = px.nextProposalN(reply.Hint) }
          px.mu.Unlock()
        }
      }

      if accOK >= px.majority(len(peers)) {
        // decided, broadcast decide
        for i := 0; i < len(peers); i++ {
          args := &DecideArgs{Seq: seq, V: useV, Done: myDone, From: me}
          var reply DecideReply
          if i == me {
            px.Decide(args, &reply)
          } else {
            call(peers[i], "Paxos.Decide", args, &reply)
          }
        }
        decided = true
        decidedVal = useV
      }
    }

    if !decided {
      // ultra-small randomized backoff to improve progress under high loss
      time.Sleep(time.Duration(1+rand.Intn(3)) * time.Millisecond)
    }
  }

  // clear running flag
  px.mu.Lock()
  delete(px.running, seq)
  // ensure local status reflects decided value
  if decided {
    inst := px.getInstance(seq)
    inst.Decided = true
    inst.Value = decidedVal
  }
  px.mu.Unlock()
}
