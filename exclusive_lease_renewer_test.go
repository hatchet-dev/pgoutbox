package pgoutbox

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestLeaseRenewer_CallsRenewRepeatedly(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	r := newLeaseRenewer(func() time.Duration { return 5 * time.Millisecond }, func(_ context.Context, topic string) {
		if topic != "orders" {
			t.Errorf("renew called with topic %q, want %q", topic, "orders")
		}
		calls.Add(1)
	})

	r.Start(context.Background(), "orders")
	defer r.Stop("orders")

	deadline := time.After(2 * time.Second)
	for calls.Load() < 3 {
		select {
		case <-deadline:
			t.Fatalf("timed out waiting for at least 3 renew calls, got %d", calls.Load())
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestLeaseRenewer_StopIsNoOpWhenNotRunning(t *testing.T) {
	t.Parallel()

	r := newLeaseRenewer(func() time.Duration { return time.Second }, func(context.Context, string) {})

	// Must return promptly without panicking, even though nothing was started.
	done := make(chan struct{})
	go func() {
		r.Stop("orders")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung on a topic that was never started")
	}
}

func TestLeaseRenewer_StopWaitsForInFlightRenewToFinish(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	release := make(chan struct{})
	var stopReturned atomic.Bool

	r := newLeaseRenewer(func() time.Duration { return time.Millisecond }, func(_ context.Context, _ string) {
		close(entered)
		<-release
		if stopReturned.Load() {
			t.Error("renew was still executing after Stop returned")
		}
	})

	r.Start(context.Background(), "orders")

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for renew to start")
	}

	stopDone := make(chan struct{})
	go func() {
		r.Stop("orders")
		stopReturned.Store(true)
		close(stopDone)
	}()

	// Stop must block while the renew call is still running.
	select {
	case <-stopDone:
		t.Fatal("Stop returned before the in-flight renew call finished")
	case <-time.After(100 * time.Millisecond):
	}

	close(release)

	select {
	case <-stopDone:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for Stop to return after unblocking renew")
	}
}

func TestLeaseRenewer_StartNeverOverlapsRenewCalls(t *testing.T) {
	t.Parallel()

	var active atomic.Int32
	r := newLeaseRenewer(func() time.Duration { return time.Millisecond }, func(_ context.Context, _ string) {
		if active.Add(1) > 1 {
			t.Error("concurrent renew calls detected for the same topic")
		}
		time.Sleep(2 * time.Millisecond)
		active.Add(-1)
	})

	r.Start(context.Background(), "orders")
	for range 5 {
		time.Sleep(3 * time.Millisecond)
		// Restarting must stop the previous goroutine (including any renew
		// call in flight) before a new one begins.
		r.Start(context.Background(), "orders")
	}
	r.Stop("orders")
}

func TestLeaseRenewer_ExternalContextCancelStopsRenewer(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32
	r := newLeaseRenewer(func() time.Duration { return 5 * time.Millisecond }, func(context.Context, string) {
		calls.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	r.Start(ctx, "orders")

	deadline := time.After(2 * time.Second)
	for calls.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for the first renew call")
		case <-time.After(5 * time.Millisecond):
		}
	}

	cancel()
	time.Sleep(20 * time.Millisecond) // let the goroutine observe ctx.Done and exit
	countAfterCancel := calls.Load()
	time.Sleep(50 * time.Millisecond)
	if calls.Load() != countAfterCancel {
		t.Fatalf("renew kept being called after ctx cancellation: before=%d after=%d", countAfterCancel, calls.Load())
	}

	// Stop must not hang even though the goroutine already exited on its own.
	done := make(chan struct{})
	go func() {
		r.Stop("orders")
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop hung after external context cancellation had already stopped the renewer")
	}
}

func TestLeaseRenewer_TopicsAreIndependent(t *testing.T) {
	t.Parallel()

	var ordersCalls, invoicesCalls atomic.Int32
	r := newLeaseRenewer(func() time.Duration { return 5 * time.Millisecond }, func(_ context.Context, topic string) {
		switch topic {
		case "orders":
			ordersCalls.Add(1)
		case "invoices":
			invoicesCalls.Add(1)
		}
	})

	r.Start(context.Background(), "orders")
	time.Sleep(30 * time.Millisecond)
	r.Stop("orders")
	before := ordersCalls.Load()

	r.Start(context.Background(), "invoices")
	defer r.Stop("invoices")
	time.Sleep(30 * time.Millisecond)

	if ordersCalls.Load() != before {
		t.Fatalf("orders renewer kept running after Stop: before=%d after=%d", before, ordersCalls.Load())
	}
	if invoicesCalls.Load() == 0 {
		t.Fatal("invoices renewer never called renew")
	}
}
