package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	currentView   View
	primaryAcked  bool
	lastPing      map[string]time.Time
	serverViewnum map[string]uint
	restarted     map[string]bool
}

func (vs *ViewServer) installViewLocked(v View) {
	vs.currentView = v
	vs.primaryAcked = false
	for name := range vs.restarted {
		if name != v.Primary && name != v.Backup {
			delete(vs.restarted, name)
		}
	}
	delete(vs.restarted, v.Primary)
	delete(vs.restarted, v.Backup)
}

func (vs *ViewServer) alive(name string, now time.Time) bool {
	t, ok := vs.lastPing[name]
	if !ok {
		return false
	}
	return now.Sub(t) < PingInterval*DeadPings
}

func (vs *ViewServer) pickIdle(now time.Time, exclude map[string]struct{}) string {
	for name := range vs.lastPing {
		if _, skip := exclude[name]; skip {
			continue
		}
		if !vs.alive(name, now) {
			continue
		}
		return name
	}
	return ""
}

func (vs *ViewServer) maybeAdvanceViewLocked(now time.Time) {
	if vs.currentView.Viewnum == 0 {
		empty := make(map[string]struct{})
		if primary := vs.pickIdle(now, empty); primary != "" {
			vs.installViewLocked(View{Viewnum: 1, Primary: primary})
		}
		return
	}

	if !vs.primaryAcked && vs.currentView.Primary != "" {
		return
	}

	primary := vs.currentView.Primary
	backup := vs.currentView.Backup

	primaryAlive := primary != "" && vs.alive(primary, now) && !vs.restarted[primary]
	backupAlive := backup != "" && vs.alive(backup, now) && !vs.restarted[backup]

	newPrimary := primary
	newBackup := backup
	changed := false

	if !primaryAlive {
		if backupAlive {
			newPrimary = backup
		} else {
			newPrimary = ""
		}
		newBackup = ""
		changed = true
	}

	if newPrimary == "" {
		return
	}

	exclude := make(map[string]struct{})
	exclude[newPrimary] = struct{}{}
	if newBackup != "" {
		exclude[newBackup] = struct{}{}
	}

	if newPrimary == backup {
		candidate := vs.pickIdle(now, exclude)
		newBackup = candidate
		if candidate != backup {
			changed = true
		}
	} else if newBackup != "" && !backupAlive {
		candidate := vs.pickIdle(now, exclude)
		newBackup = candidate
		changed = true
	} else if newBackup == "" {
		candidate := vs.pickIdle(now, exclude)
		if candidate != "" {
			newBackup = candidate
			changed = true
		}
	}

	if !changed {
		return
	}

	next := View{Viewnum: vs.currentView.Viewnum + 1, Primary: newPrimary, Backup: newBackup}
	vs.installViewLocked(next)
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.lastPing == nil {
		vs.lastPing = make(map[string]time.Time)
	}
	if vs.serverViewnum == nil {
		vs.serverViewnum = make(map[string]uint)
	}
	if vs.restarted == nil {
		vs.restarted = make(map[string]bool)
	}

	now := time.Now()
	oldView := vs.serverViewnum[args.Me]

	vs.lastPing[args.Me] = now
	vs.serverViewnum[args.Me] = args.Viewnum

	if args.Me == vs.currentView.Primary && args.Viewnum == vs.currentView.Viewnum {
		vs.primaryAcked = true
		vs.restarted[args.Me] = false
	}

	if args.Me == vs.currentView.Backup && args.Viewnum == vs.currentView.Viewnum {
		vs.restarted[args.Me] = false
	}

	if args.Viewnum == 0 && oldView > 0 {
		if args.Me == vs.currentView.Primary || args.Me == vs.currentView.Backup {
			vs.restarted[args.Me] = true
		}
	}

	vs.maybeAdvanceViewLocked(now)
	reply.View = vs.currentView
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	reply.View = vs.currentView
	vs.mu.Unlock()
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	if vs.lastPing == nil {
		return
	}
	vs.maybeAdvanceViewLocked(time.Now())
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{}
	vs.primaryAcked = false
	vs.lastPing = make(map[string]time.Time)
	vs.serverViewnum = make(map[string]uint)
	vs.restarted = make(map[string]bool)

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
