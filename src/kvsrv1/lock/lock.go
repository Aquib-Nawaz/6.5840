package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"log"
	"math/rand"
	"time"
)

const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	uid string
	l   string
}

type State string

const (
	Free   = "free"
	Locked = "locked"
)

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, l: l, uid: generateRandomString(10)}
	// You may add code here
	lk.ck.Put(lk.l, Free, 0)
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	DPrintf("UID: %v Acquire func called\n", lk.uid)
	for {
		val, ver, err := lk.ck.Get(lk.l)
		if err != rpc.OK {
			panic(err)
		}
		if val == Free {
			err = lk.ck.Put(lk.l, lk.uid, ver)
			if err == rpc.OK {
				DPrintf("UID: %v Lock Acquired\n", lk.uid)
				break
			}
			if err == rpc.ErrMaybe {
				val, _, _ := lk.ck.Get(lk.l)
				DPrintf("UID: %v Making sure that this client has acquired lock, got val %v\n", lk.uid, val)
				if val == lk.uid {
					break
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	// Your code here
	val, ver, err := lk.ck.Get(lk.l)
	if err != rpc.OK {
		panic(err)
	}
	if val == lk.uid {
		err = lk.ck.Put(lk.l, Free, ver)
		if err != rpc.OK && err != rpc.ErrMaybe {
			panic(err)
		}
		DPrintf("UID: %v Lock Released\n", lk.uid)
	} else {
		panic("Releasing Lock without being acquired")
	}
}

func generateRandomString(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Int63()%int64(len(letterBytes))]
	}
	return string(b)
}
