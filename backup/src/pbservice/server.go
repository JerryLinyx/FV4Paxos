package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
  l          net.Listener
  dead       bool // for testing
  unreliable bool // for testing
  me         string
  vs         *viewservice.Clerk
  done       sync.WaitGroup
  finish     chan interface{}
  // Your declarations here.
  mu           sync.Mutex
  opsMu        sync.Mutex
  view         viewservice.View
  kv           map[string]string
  results      map[int64]OpResult
  backupSynced bool
  lastPingOK   bool
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
    // 序列化客户端请求，避免并发计算导致顺序不一致
    pb.opsMu.Lock()
    defer pb.opsMu.Unlock()
    pb.mu.Lock()
    defer pb.mu.Unlock()

	if args.Forwarded {
		if pb.view.Backup != pb.me || pb.view.Primary != args.Primary || pb.view.Viewnum != args.Viewnum {
			reply.Err = ErrWrongServer
			return nil
		}
    } else {
        if pb.view.Primary != pb.me || !pb.lastPingOK {
            reply.Err = ErrWrongServer
            return nil
        }
        if pb.view.Backup != "" && pb.view.Backup != pb.me && !pb.backupSynced {
            reply.Err = ErrWrongServer
            return nil
        }
    }

	stored, hasStored := pb.results[args.ClientID]
	if hasStored {
		if args.RequestID < stored.RequestID {
			reply.Err = ErrWrongServer
			return nil
		}
	}

    duplicate := hasStored && stored.RequestID == args.RequestID && stored.Op == "Put"

    // duplicate 请求：直接返回缓存结果，不转发
    if duplicate {
        reply.Err = stored.Err
        reply.PreviousValue = stored.Value
        return nil
    }

    // 计算最终值（包括 DoHash 的情况）
    old := pb.kv[args.Key]
    prev := old
    var newVal string
    if args.DoHash {
        newVal = strconv.Itoa(int(hash(old + args.Value)))
    } else {
        newVal = args.Value
    }

    // 将最终值转发给备机并等待确认，避免主备不一致
    if !args.Forwarded && pb.view.Backup != "" && pb.view.Backup != pb.me {
        fArgs := *args
        fArgs.Forwarded = true
        fArgs.Primary = pb.me
        fArgs.Viewnum = pb.view.Viewnum
        // 发送最终值，备机直接落盘，不再根据旧值计算
        fArgs.Value = newVal
        fArgs.DoHash = false

        backup := pb.view.Backup
        viewnum := pb.view.Viewnum
        for {
            var fReply PutReply
            // 释放锁进行 RPC，避免阻塞 tick/ping
            pb.mu.Unlock()
            ok := call(backup, "PBServer.Put", &fArgs, &fReply)
            pb.mu.Lock()

            // 视图发生变化：让客户端按新主重试
            if pb.view.Viewnum != viewnum || pb.view.Backup != backup || pb.view.Primary != pb.me {
                reply.Err = ErrWrongServer
                return nil
            }
            if ok && fReply.Err == OK {
                break
            }
            // 临时失败（丢包/断开），稍等重试直到成功
            pb.mu.Unlock()
            time.Sleep(viewservice.PingInterval / 2)
            pb.mu.Lock()
        }
    }

    // 本地提交写入并记录幂等结果
    pb.kv[args.Key] = newVal
    pb.results[args.ClientID] = OpResult{RequestID: args.RequestID, Value: prev, Err: OK, Op: "Put"}
    reply.Err = OK
    reply.PreviousValue = prev
    return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
    // 序列化客户端请求，避免与 Put 并发造成读到中间状态
    pb.opsMu.Lock()
    defer pb.opsMu.Unlock()
    pb.mu.Lock()
    defer pb.mu.Unlock()

	if args.Forwarded {
		if pb.view.Backup != pb.me || pb.view.Primary != args.Primary || pb.view.Viewnum != args.Viewnum {
			reply.Err = ErrWrongServer
			return nil
		}
    } else {
        if pb.view.Primary != pb.me || !pb.lastPingOK {
            reply.Err = ErrWrongServer
            return nil
        }
        if pb.view.Backup != "" && pb.view.Backup != pb.me && !pb.backupSynced {
            reply.Err = ErrWrongServer
            return nil
        }
    }

	stored, hasStored := pb.results[args.ClientID]
	if hasStored {
		if args.RequestID < stored.RequestID {
			reply.Err = ErrWrongServer
			return nil
		}
	}

    duplicate := hasStored && stored.RequestID == args.RequestID && stored.Op == "Get"

    // duplicate 直接返回缓存结果
    if duplicate {
        reply.Err = stored.Err
        reply.Value = stored.Value
        return nil
    }

    // 本地读取；读不转发
    var value string
    var errCode Err
    if v, ok := pb.kv[args.Key]; ok {
        value = v
        errCode = OK
    } else {
        value = ""
        errCode = ErrNoKey
    }

    reply.Err = errCode
    reply.Value = value
    pb.results[args.ClientID] = OpResult{RequestID: args.RequestID, Value: value, Err: errCode, Op: "Get"}

    return nil
}

func (pb *PBServer) SyncState(args *SyncArgs, reply *SyncReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if pb.view.Backup != pb.me || pb.view.Primary != args.Primary || pb.view.Viewnum != args.Viewnum {
		reply.Err = ErrWrongServer
		return nil
	}

	pb.kv = make(map[string]string)
	for k, v := range args.Data {
		pb.kv[k] = v
	}

	pb.results = make(map[int64]OpResult)
	for id, res := range args.Results {
		pb.results[id] = res
	}

	reply.Err = OK
	return nil
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
    pb.mu.Lock()
    viewnum := pb.view.Viewnum
    pb.mu.Unlock()

    newView, err := pb.vs.Ping(viewnum)
    if err != nil {
        pb.mu.Lock()
        pb.lastPingOK = false
        pb.mu.Unlock()
        return
    }

    pb.mu.Lock()
    pb.lastPingOK = true
    if newView.Viewnum != pb.view.Viewnum || newView.Primary != pb.view.Primary || newView.Backup != pb.view.Backup {
        pb.view = newView
        if pb.view.Primary == pb.me {
            if pb.view.Backup == "" || pb.view.Backup == pb.me {
                pb.backupSynced = true
            } else {
                pb.backupSynced = false
            }
        } else {
            pb.backupSynced = false
        }
    }

	needSync := pb.view.Primary == pb.me && pb.view.Backup != "" && pb.view.Backup != pb.me && !pb.backupSynced
	var data map[string]string
	var results map[int64]OpResult
	backup := pb.view.Backup
	currentViewnum := pb.view.Viewnum
	primary := pb.me
	if needSync {
		data = make(map[string]string, len(pb.kv))
		for k, v := range pb.kv {
			data[k] = v
		}
		results = make(map[int64]OpResult, len(pb.results))
		for id, res := range pb.results {
			results[id] = res
		}
	}
	pb.mu.Unlock()

	if needSync {
		args := &SyncArgs{Data: data, Results: results, Viewnum: currentViewnum, Primary: primary}
		var reply SyncReply
		ok := call(backup, "PBServer.SyncState", args, &reply)
		if ok && reply.Err == OK {
			pb.mu.Lock()
			if pb.view.Primary == primary && pb.view.Backup == backup && pb.view.Viewnum == currentViewnum {
				pb.backupSynced = true
			}
			pb.mu.Unlock()
		}
	}
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.kv = make(map[string]string)
  pb.results = make(map[int64]OpResult)
  pb.view = viewservice.View{}
    pb.backupSynced = true
    pb.lastPingOK = false

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
