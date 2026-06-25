package pgoutbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/pgoutbox"
	"github.com/hatchet-dev/pgoutbox/sqlc"
)

func TestExclusiveConsumer_AcquireSucceeds(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	require.NoError(t, ob.AcquireTopic(ctx, "orders"))
}

func TestExclusiveConsumer_AcquireBeforeMessagesExist(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	// AcquireTopic should create the topics row even when no messages exist yet.
	require.NoError(t, ob.AcquireTopic(ctx, "orders"))

	ob.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, ob, "orders", 2)

	msgs, err := ob.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, msgs, 2)
}

func TestExclusiveConsumer_RenewsInBackground(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	// Use a very short lease that would expire without renewal, and a renewal
	// interval that fires well before expiry.
	restoreDuration := pgoutbox.SetExclusiveLeaseDurationForTest(200 * time.Millisecond)
	defer restoreDuration()
	restoreRenew := pgoutbox.SetExclusiveLeaseRenewIntervalForTest(50 * time.Millisecond)
	defer restoreRenew()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ob.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, ob, "orders", 2)

	require.NoError(t, ob.AcquireTopic(ctx, "orders"))

	// Wait long enough that the original lease would have expired without renewal.
	time.Sleep(400 * time.Millisecond)

	// Renewer should have kept the lease alive; ProcessMessages must succeed.
	msgs, err := ob.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, msgs, 2)
}

func TestExclusiveConsumer_BlocksUntilOtherLeaseExpires(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	// Short lease so A's hold expires quickly after its context is cancelled.
	restoreDuration := pgoutbox.SetExclusiveLeaseDurationForTest(150 * time.Millisecond)
	defer restoreDuration()
	// Disable background renewal (interval longer than the test).
	restoreRenew := pgoutbox.SetExclusiveLeaseRenewIntervalForTest(10 * time.Minute)
	defer restoreRenew()
	// Fast retry so B picks up the lease quickly after A's expires.
	restoreRetry := pgoutbox.SetExclusiveLeaseRetryIntervalForTest(20 * time.Millisecond)
	defer restoreRetry()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	obA, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	obB, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ctxA, cancelA := context.WithCancel(ctx)
	require.NoError(t, obA.AcquireTopic(ctxA, "orders"))

	// B's AcquireTopic must block while A holds the lease.
	acquired := make(chan error, 1)
	go func() { acquired <- obB.AcquireTopic(ctx, "orders") }()

	// Verify B has not acquired yet.
	select {
	case err := <-acquired:
		t.Fatalf("B acquired the lease unexpectedly early: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	// Cancel A's context — the renewer stops, the 150ms lease will expire.
	cancelA()

	// B should acquire once the lease lapses.
	select {
	case err := <-acquired:
		require.NoError(t, err, "B should acquire the lease after A's expires")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for B to acquire the lease")
	}
}

func TestExclusiveConsumer_ContextCancelledWhileBlocking(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreDuration := pgoutbox.SetExclusiveLeaseDurationForTest(10 * time.Minute)
	defer restoreDuration()
	restoreRetry := pgoutbox.SetExclusiveLeaseRetryIntervalForTest(20 * time.Millisecond)
	defer restoreRetry()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	obA, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	obB, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	require.NoError(t, obA.AcquireTopic(ctx, "orders"))

	// B tries to acquire but with a context that expires quickly.
	ctxB, cancelB := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelB()

	err = obB.AcquireTopic(ctxB, "orders")
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.DeadlineExceeded))
}

func TestExclusiveConsumer_ProcessMessagesSucceedsWhenHoldingLease(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ob.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, ob, "orders", 2)

	require.NoError(t, ob.AcquireTopic(ctx, "orders"))

	msgs, err := ob.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, msgs, 2)
}

func TestExclusiveConsumer_ProcessMessagesFailsWhenHeldByOther(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	obA, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	obB, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	obB.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, obA, "orders", 2)

	require.NoError(t, obA.AcquireTopic(ctx, "orders"))

	_, err = obB.ProcessMessages(ctx, "orders")
	require.Error(t, err)
	assert.True(t, errors.Is(err, pgoutbox.ErrExclusiveLeaseHeld))
}

func TestExclusiveConsumer_ProcessMessagesFailsWithExpiredLease(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	// Short lease, renewal disabled — the lease expires after cancelling acquireCtx.
	restoreDuration := pgoutbox.SetExclusiveLeaseDurationForTest(100 * time.Millisecond)
	defer restoreDuration()
	restoreRenew := pgoutbox.SetExclusiveLeaseRenewIntervalForTest(10 * time.Minute)
	defer restoreRenew()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ob.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, ob, "orders", 2)

	acquireCtx, cancelAcquire := context.WithCancel(ctx)
	require.NoError(t, ob.AcquireTopic(acquireCtx, "orders"))

	// Stop the renewer and wait for the short lease to expire.
	cancelAcquire()
	time.Sleep(200 * time.Millisecond)

	_, err = ob.ProcessMessages(ctx, "orders")
	require.Error(t, err, "ProcessMessages must fail when own lease has expired")
}

func TestExclusiveConsumer_ProcessMessagesNonExclusiveTopicUnaffected(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ob.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, ob, "orders", 3)

	// No AcquireTopic call — topic has no exclusive consumer.
	msgs, err := ob.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, msgs, 3)
}

// noopFlusher is a Flusher that does nothing and always succeeds.
type noopFlusher struct{}

func (f *noopFlusher) Flush(_ context.Context, _ []*sqlc.Message) error {
	return nil
}
