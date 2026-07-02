package pgoutbox

import (
	"sync/atomic"
	"time"
)

// atomicDuration is a time.Duration that can be read from background
// goroutines while tests concurrently override it, without a data race.
type atomicDuration struct {
	ns atomic.Int64
}

func newAtomicDuration(d time.Duration) *atomicDuration {
	a := &atomicDuration{}
	a.ns.Store(int64(d))
	return a
}

func (a *atomicDuration) Load() time.Duration {
	return time.Duration(a.ns.Load())
}

func (a *atomicDuration) Store(d time.Duration) {
	a.ns.Store(int64(d))
}
