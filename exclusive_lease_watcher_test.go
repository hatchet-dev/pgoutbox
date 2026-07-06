package pgoutbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaseWatcher_FiresOnceOnExternalCancel(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	w := newLeaseWatcher(func(_ context.Context, topic string) {
		if topic != "orders" {
			t.Errorf("onCancel called with topic %q, want %q", topic, "orders")
		}
		calls.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx, "orders")

	if got := calls.Load(); got != 0 {
		t.Fatalf("onCancel fired before the ctx ended: %d calls", got)
	}

	cancel()

	deadline := time.After(2 * time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for onCancel to fire after ctx cancellation")
		case <-time.After(5 * time.Millisecond):
		}
	}

	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("onCancel fired %d times, want exactly 1", got)
	}
}

func TestLeaseWatcher_OnCancelContextIsDetached(t *testing.T) {
	t.Parallel()

	fired := make(chan error, 1)
	w := newLeaseWatcher(func(ctx context.Context, _ string) {
		fired <- ctx.Err()
	})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx, "orders")
	cancel()

	select {
	case err := <-fired:
		if err != nil {
			t.Fatalf("onCancel received an already-ended context: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for onCancel to fire")
	}
}

// TestLeaseWatcher_StopSuppressesOnCancel: a deliberate Stop must not fire
// onCancel — callers stop a watcher exactly when they are about to write
// lease state themselves, and Stop must not block on a database write it
// doesn't need.
func TestLeaseWatcher_StopSuppressesOnCancel(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	w := newLeaseWatcher(func(context.Context, string) {
		calls.Add(1)
	})

	w.Start(context.Background(), "orders")
	w.Stop("orders")

	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 0 {
		t.Fatalf("onCancel fired %d times after a deliberate Stop, want 0", got)
	}
}

func TestLeaseWatcher_StopIsNoOpWhenNotRunning(t *testing.T) {
	t.Parallel()

	w := newLeaseWatcher(func(context.Context, string) {})

	// Must return promptly without panicking, even though nothing was started.
	done := make(chan struct{})
	go func() {
		w.Stop("orders")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung on a topic that was never started")
	}
}

// TestLeaseWatcher_StopBlocksUntilInFlightOnCancelFinishes covers the
// guarantee callers depend on for lease-state ordering: when the watched ctx
// ends just before Stop is called, Stop must not return until the already
// in-flight onCancel has fully finished, so anything the caller writes
// afterwards has the last word.
func TestLeaseWatcher_StopBlocksUntilInFlightOnCancelFinishes(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	var stopReturned atomic.Bool

	w := newLeaseWatcher(func(_ context.Context, _ string) {
		close(entered)
		<-release
		if stopReturned.Load() {
			t.Error("onCancel was still executing after Stop returned")
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx, "orders")

	// External cancellation puts onCancel in flight before Stop is called.
	cancel()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for onCancel to start")
	}

	stopDone := make(chan struct{})
	go func() {
		w.Stop("orders")
		stopReturned.Store(true)
		close(stopDone)
	}()

	// Stop must block while onCancel is still running.
	select {
	case <-stopDone:
		t.Fatal("Stop returned before the in-flight onCancel finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Stop to return after unblocking onCancel")
	}
}

// TestLeaseWatcher_StartRestartsWithoutFiring: restarting a topic stops the
// previous watcher without firing its onCancel; only the final watcher's
// external cancellation fires, exactly once.
func TestLeaseWatcher_StartRestartsWithoutFiring(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	w := newLeaseWatcher(func(context.Context, string) {
		calls.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(context.Background(), "orders")
	for range 5 {
		w.Start(context.Background(), "orders")
	}
	w.Start(ctx, "orders")

	if got := calls.Load(); got != 0 {
		t.Fatalf("onCancel fired %d times across restarts, want 0", got)
	}

	cancel()

	deadline := time.After(2 * time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the final watcher to fire")
		case <-time.After(5 * time.Millisecond):
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := calls.Load(); got != 1 {
		t.Fatalf("onCancel fired %d times, want exactly 1 (the final watcher)", got)
	}
}

// TestLeaseWatcher_ExternalContextCancelCleansUpRunningMap guards against a
// leak: if a topic's ctx is cancelled externally (not via Stop), the running
// goroutine must remove its own entry from w.running once it exits, so
// running reflects "currently running" rather than "ever started" and Stop
// (or a later Start) doesn't have to be called just to reclaim the memory.
func TestLeaseWatcher_ExternalContextCancelCleansUpRunningMap(t *testing.T) {
	t.Parallel()

	w := newLeaseWatcher(func(context.Context, string) {})

	ctx, cancel := context.WithCancel(context.Background())
	w.Start(ctx, "orders")
	cancel()

	deadline := time.After(2 * time.Second)
	for {
		if _, ok := w.running.Load("orders"); !ok {
			break
		}
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the running-map entry to be cleaned up after external cancellation")
		case <-time.After(5 * time.Millisecond):
		}
	}

	// Stop must not hang even though the goroutine already exited on its own.
	done := make(chan struct{})
	go func() {
		w.Stop("orders")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung after external context cancellation had already stopped the watcher")
	}
}

func TestLeaseWatcher_TopicsAreIndependent(t *testing.T) {
	t.Parallel()

	var ordersCalls, invoicesCalls atomic.Int32
	w := newLeaseWatcher(func(_ context.Context, topic string) {
		switch topic {
		case "orders":
			ordersCalls.Add(1)
		case "invoices":
			invoicesCalls.Add(1)
		}
	})

	ordersCtx, cancelOrders := context.WithCancel(context.Background())
	w.Start(ordersCtx, "orders")
	w.Start(context.Background(), "invoices")

	// Cancelling orders fires only the orders watcher.
	cancelOrders()
	deadline := time.After(2 * time.Second)
	for ordersCalls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the orders watcher to fire")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if got := invoicesCalls.Load(); got != 0 {
		t.Fatalf("cancelling orders fired the invoices watcher: %d calls", got)
	}

	// Stopping invoices fires nothing.
	w.Stop("invoices")
	time.Sleep(50 * time.Millisecond)
	if got := invoicesCalls.Load(); got != 0 {
		t.Fatalf("stopping invoices fired its onCancel: %d calls", got)
	}
}
