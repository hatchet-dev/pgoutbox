package pgoutbox

import (
	"context"
	"errors"
	"sync"
)

// errLeaseWatcherStopped marks a watcher cancellation as a deliberate Stop,
// which suppresses onCancel, as opposed to the watched ctx ending on its own.
var errLeaseWatcherStopped = errors.New("lease watcher stopped")

// leaseWatcher manages one background goroutine per topic that waits for the
// topic's ctx to end and then calls onCancel exactly once with a detached
// (context.WithoutCancel) context, so the call can still reach the database
// after cancellation. A deliberate Stop suppresses onCancel: callers stop a
// watcher precisely when they are about to write lease state themselves
// (release, re-acquire), making the watcher's write pointless — and Stop must
// not block on a detached, unbounded database call it doesn't need. The
// watcher exists standalone (rather than inline in AcquireTopic/ReleaseTopic)
// so its start/stop/restart semantics can be unit tested without a database.
//
// Stop blocks until the topic's goroutine has fully exited before returning —
// including an onCancel call already in flight from the watched ctx ending
// concurrently. That guarantee matters to callers: onCancel's one action is
// writing lease state, so a caller that stops the watcher before writing its
// own lease state is guaranteed to have the last word. Start relies on the
// same guarantee to restart a topic without ever running two watchers for it
// concurrently.
type leaseWatcher struct {
	onCancel func(ctx context.Context, topic string)

	running sync.Map // topic string -> *leaseWatcherHandle
}

type leaseWatcherHandle struct {
	cancel context.CancelCauseFunc
	done   chan struct{}
}

// newLeaseWatcher creates a leaseWatcher that calls onCancel(detachedCtx,
// topic) once per Start, when that topic's ctx ends for any reason other
// than a deliberate Stop.
func newLeaseWatcher(onCancel func(ctx context.Context, topic string)) *leaseWatcher {
	return &leaseWatcher{onCancel: onCancel}
}

// Start begins watching topic, deriving its lifetime from ctx. If topic is
// already being watched, that goroutine is stopped (and waited for) before
// the new one starts, so exactly one watcher ever runs per topic at a time.
func (w *leaseWatcher) Start(ctx context.Context, topic string) {
	w.Stop(topic)

	watchCtx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	handle := &leaseWatcherHandle{cancel: cancel, done: done}
	w.running.Store(topic, handle)

	go func() {
		// Registered before the CompareAndDelete defer below so it runs after
		// it (defers run LIFO): any waiter unblocked by close(done) is
		// guaranteed to observe the map already cleaned up.
		defer close(done)
		// Runs after the watch ends for any reason, including the ctx passed
		// to Start being cancelled externally rather than via Stop. Without
		// this, running would keep a stale entry for a watcher that already
		// exited, tracking "ever started" instead of "currently running".
		// Guarded by identity so it never removes a different handle a later
		// Start/Stop may have already installed for the same topic.
		defer w.running.CompareAndDelete(topic, handle)
		<-watchCtx.Done()
		// If the watched ctx ended on its own in the instant before Stop
		// cancelled us, the original cause wins and onCancel fires — Stop
		// waits for it, and every stopping caller writes its own lease state
		// afterwards, so the late fire is always superseded.
		if errors.Is(context.Cause(watchCtx), errLeaseWatcherStopped) {
			return
		}
		w.onCancel(context.WithoutCancel(watchCtx), topic)
	}()
}

// Stop cancels topic's watcher goroutine, if one is running, suppresses its
// onCancel, and blocks until it has exited. It is a no-op if no watcher is
// running for topic, including when one was running but has already fired on
// its own (e.g. because the ctx passed to Start was cancelled externally).
func (w *leaseWatcher) Stop(topic string) {
	v, ok := w.running.LoadAndDelete(topic)
	if !ok {
		return
	}
	handle := v.(*leaseWatcherHandle)
	handle.cancel(errLeaseWatcherStopped)
	<-handle.done
}
