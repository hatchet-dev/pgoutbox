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

func TestExclusiveConsumer_ReleaseTopicAllowsImmediateReacquire(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	// Long lease duration: if ReleaseTopic didn't clear the lease immediately,
	// B would have to wait out this whole duration to acquire.
	restoreDuration := pgoutbox.SetExclusiveLeaseDurationForTest(10 * time.Minute)
	defer restoreDuration()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	obA, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)
	obB, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	require.NoError(t, obA.AcquireTopic(ctx, "orders"))
	require.NoError(t, obA.ReleaseTopic(ctx, "orders"))

	// B must be able to acquire right away, not after waiting out A's
	// 10-minute lease.
	acquireCtx, acquireCancel := context.WithTimeout(ctx, 2*time.Second)
	defer acquireCancel()
	require.NoError(t, obB.AcquireTopic(acquireCtx, "orders"))
}

func TestExclusiveConsumer_ReleaseTopicNoOpWhenNotHeld(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	// Never acquired — releasing must be a harmless no-op, even before the
	// topics row exists.
	require.NoError(t, ob.ReleaseTopic(ctx, "orders"))
}

func TestExclusiveConsumer_ReleaseTopicRevokesProcessMessagesAccess(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ob.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, ob, "orders", 2)

	require.NoError(t, ob.AcquireTopic(ctx, "orders"))
	require.NoError(t, ob.ReleaseTopic(ctx, "orders"))

	_, err = ob.ProcessMessages(ctx, "orders")
	require.Error(t, err, "ProcessMessages must fail after the lease has been released")
}

// noopFlusher is a Flusher that does nothing and always succeeds.
type noopFlusher struct{}

func (f *noopFlusher) Flush(_ pgoutbox.FlushContext, _ []*sqlc.Message) error {
	return nil
}

// blockingFlusher signals entered once Flush is called, then blocks until
// proceed is closed. It lets a test pause a ProcessMessages call in the
// middle of its transaction (messages already locked, delete not yet issued)
// so another goroutine can act while the first is stalled.
type blockingFlusher struct {
	entered chan struct{}
	proceed chan struct{}
}

func (f *blockingFlusher) Flush(_ pgoutbox.FlushContext, _ []*sqlc.Message) error {
	close(f.entered)
	<-f.proceed
	return nil
}

// TestExclusiveConsumer_ProcessMessagesBlocksHandoffUntilTransactionEnds
// guards against a race reported against the exclusive-consumer feature:
// cycling consumers could let a superseded ("zombie") consumer finish
// processing and deleting messages after a new consumer had already acquired
// the topic.
//
// The fix locks the topic row FOR UPDATE inside ProcessMessages' own
// transaction (checkExclusiveAccessForUpdate), for the lifetime of that
// transaction. AcquireTopic takes the same row lock to hand off the lease, so
// the two now serialize on Postgres row locking instead of racing on a
// timestamp read that can go stale between the check and the commit.
//
// This test pauses consumer A inside Flush (after it has locked the topic row
// and the messages, before commit) well past the point its lease would have
// expired by timestamp alone, and asserts that consumer B's AcquireTopic
// cannot complete until A's transaction actually ends — proving there is no
// window where both A and B can believe they hold the lease at once.
func TestExclusiveConsumer_ProcessMessagesBlocksHandoffUntilTransactionEnds(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	// Short lease so it would have expired by timestamp alone while A is
	// stalled; renewal disabled so nothing keeps it alive artificially.
	restoreDuration := pgoutbox.SetExclusiveLeaseDurationForTest(100 * time.Millisecond)
	defer restoreDuration()
	restoreRenew := pgoutbox.SetExclusiveLeaseRenewIntervalForTest(10 * time.Minute)
	defer restoreRenew()
	restoreRetry := pgoutbox.SetExclusiveLeaseRetryIntervalForTest(20 * time.Millisecond)
	defer restoreRetry()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	obA, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)
	obB, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	entered := make(chan struct{})
	proceed := make(chan struct{})
	obA.AddFlusher("orders", &blockingFlusher{entered: entered, proceed: proceed})
	obB.AddFlusher("orders", &noopFlusher{})

	insertMessages(t, ctx, obA, "orders", 2)

	require.NoError(t, obA.AcquireTopic(ctx, "orders"))

	// A passes the exclusive-access check (locking the topic row) and locks
	// the messages, then stalls inside Flush with both locks still held.
	processDone := make(chan error, 1)
	go func() {
		_, err := obA.ProcessMessages(ctx, "orders")
		processDone <- err
	}()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for A to enter Flush")
	}

	// A's lease is not being renewed, so by timestamp alone it has expired.
	// B tries to acquire in the background; it must not succeed while A's
	// transaction (and thus its row lock) is still open.
	time.Sleep(150 * time.Millisecond)

	acquiredB := make(chan error, 1)
	go func() { acquiredB <- obB.AcquireTopic(ctx, "orders") }()

	select {
	case err := <-acquiredB:
		t.Fatalf("B must not acquire the topic while A's ProcessMessages transaction is still open (err=%v)", err)
	case <-time.After(300 * time.Millisecond):
	}

	// Unblock A. Since it held the row lock for its entire transaction, no
	// other instance could have taken the lease out from under it, so its
	// commit is safe and must succeed.
	close(proceed)

	select {
	case err := <-processDone:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for A's ProcessMessages to finish")
	}

	// Only once A's transaction has ended can B acquire the topic.
	select {
	case err := <-acquiredB:
		require.NoError(t, err, "B should acquire the topic once A's transaction has ended")
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for B to acquire the topic after A finished")
	}

	// The messages were already processed and deleted by A while it
	// legitimately held the lease; B, now the new owner, sees none left.
	msgsForB, err := obB.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Empty(t, msgsForB)
}
