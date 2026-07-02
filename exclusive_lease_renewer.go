package pgoutbox

import (
	"context"
	"sync"
	"time"
)

// leaseRenewer runs a per-topic background goroutine that calls a renew
// function on a fixed interval until stopped. It exists standalone (rather
// than inline in AcquireTopic/ReleaseTopic) so its start/stop/restart
// semantics can be unit tested without a database.
//
// Stop blocks until the topic's goroutine has fully exited before returning,
// including any renew call already in progress. That guarantee matters to
// callers: without it, a renewal already in flight could complete after the
// caller has moved on to clear or reassign the lease, undoing that change.
// Start relies on the same guarantee to restart a topic without ever running
// two renewers for it concurrently.
type leaseRenewer struct {
	// interval is called on every tick rather than captured once, so callers
	// can back it with a value that changes at runtime (e.g. in tests).
	interval func() time.Duration
	renew    func(ctx context.Context, topic string)

	running sync.Map // topic string -> *leaseRenewerHandle
}

type leaseRenewerHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// newLeaseRenewer creates a leaseRenewer that calls renew(ctx, topic) every
// interval() for each topic started via Start, until that topic is stopped or
// the ctx passed to Start is cancelled.
func newLeaseRenewer(interval func() time.Duration, renew func(ctx context.Context, topic string)) *leaseRenewer {
	return &leaseRenewer{interval: interval, renew: renew}
}

// Start begins renewing topic in the background, deriving its lifetime from
// ctx. If topic is already being renewed, that goroutine is stopped (and
// waited for) before the new one starts, so exactly one renewer ever runs per
// topic at a time.
func (r *leaseRenewer) Start(ctx context.Context, topic string) {
	r.Stop(topic)

	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	handle := &leaseRenewerHandle{cancel: cancel, done: done}
	r.running.Store(topic, handle)

	go func() {
		// Registered before the CompareAndDelete defer below so it runs after
		// it (defers run LIFO): any waiter unblocked by close(done) is
		// guaranteed to observe the map already cleaned up.
		defer close(done)
		// Runs after r.run returns for any reason, including the ctx passed to
		// Start being cancelled externally rather than via Stop. Without this,
		// running would keep a stale entry for a renewer that already exited,
		// tracking "ever started" instead of "currently running". Guarded by
		// identity so it never removes a different handle a later Start/Stop
		// may have already installed for the same topic.
		defer r.running.CompareAndDelete(topic, handle)
		r.run(renewCtx, topic)
	}()
}

// Stop cancels topic's renewer goroutine, if one is running, and blocks until
// it has exited. It is a no-op if no renewer is running for topic - including
// when one was running but has already stopped on its own (e.g. because the
// ctx passed to Start was cancelled externally).
func (r *leaseRenewer) Stop(topic string) {
	v, ok := r.running.LoadAndDelete(topic)
	if !ok {
		return
	}
	handle := v.(*leaseRenewerHandle)
	handle.cancel()
	<-handle.done
}

func (r *leaseRenewer) run(ctx context.Context, topic string) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(r.interval()):
		}
		r.renew(ctx, topic)
	}
}
