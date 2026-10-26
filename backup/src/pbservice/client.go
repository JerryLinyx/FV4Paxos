package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "time"
import "crypto/rand"
import "math/big"

// You'll probably need to uncomment these:
// import "time"
// import "crypto/rand"
// import "math/big"

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
	primary   string
	clientID  int64
	requestID int64
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here
	ck.clientID = nrand()

	return ck
}

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

// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
func (ck *Clerk) Get(key string) string {
	reqID := ck.nextRequestID()
	for {
		primary := ck.primary
		if primary == "" {
			primary = ck.vs.Primary()
			ck.primary = primary
		}
		if primary == "" {
			time.Sleep(viewservice.PingInterval)
			continue
		}

		args := &GetArgs{}
		args.Key = key
		args.ClientID = ck.clientID
		args.RequestID = reqID
		var reply GetReply

		ok := call(primary, "PBServer.Get", args, &reply)
		if ok {
			if reply.Err == OK {
				return reply.Value
			}
			if reply.Err == ErrNoKey {
				return ""
			}
			if reply.Err == ErrWrongServer {
				ck.primary = ""
				time.Sleep(viewservice.PingInterval)
				continue
			}
		} else {
			ck.primary = ""
			time.Sleep(viewservice.PingInterval)
		}
	}
}

// tell the primary to update key's value.
// must keep trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	reqID := ck.nextRequestID()
	for {
		primary := ck.primary
		if primary == "" {
			primary = ck.vs.Primary()
			ck.primary = primary
		}
		if primary == "" {
			time.Sleep(viewservice.PingInterval)
			continue
		}

		args := &PutArgs{}
		args.Key = key
		args.Value = value
		args.DoHash = dohash
		args.ClientID = ck.clientID
		args.RequestID = reqID
		var reply PutReply

		ok := call(primary, "PBServer.Put", args, &reply)
		if ok {
			if reply.Err == OK {
				return reply.PreviousValue
			}
			if reply.Err == ErrWrongServer {
				ck.primary = ""
				time.Sleep(viewservice.PingInterval)
				continue
			}
		} else {
			ck.primary = ""
			time.Sleep(viewservice.PingInterval)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}

func (ck *Clerk) nextRequestID() int64 {
	ck.requestID++
	return ck.requestID
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
