package pgoutbox_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
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

func TestExclusiveConsumer_ReleaseTopicRejectsEmptyTopic(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	err = ob.ReleaseTopic(ctx, "")
	require.Error(t, err, "ReleaseTopic must validate topic the same way AcquireTopic does")
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

	// Short lease so A's hold would have expired by timestamp alone while A is
	// stalled; the session heartbeat is disabled so nothing keeps it alive
	// artificially (simulating a hung instance whose heartbeats have stopped).
	restoreDuration := pgoutbox.SetExclusiveLeaseDurationForTest(200 * time.Millisecond)
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

	// A's consumer session is not being renewed, so by timestamp alone its
	// lease has expired. B tries to acquire in the background; it must not
	// succeed while A's transaction (and thus its row lock) is still open.
	time.Sleep(250 * time.Millisecond)

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
	// (B's own consumer session is just as stale as A's under the disabled
	// heartbeat, so assert via SQL rather than a ProcessMessages call.)
	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"))
}

// TestExclusiveConsumer_ManyTopicsBoundedSessionWrites is the regression test
// for the scaling problem session leases exist to fix: holding N topics must
// not cost N periodic writes to the topics table. It proves the topics rows
// are never touched after acquisition (via their xmin system column, which
// any UPDATE would change — even one writing identical values) while the
// single consumer-session row keeps being renewed.
func TestExclusiveConsumer_ManyTopicsBoundedSessionWrites(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreRenew := pgoutbox.SetExclusiveLeaseRenewIntervalForTest(50 * time.Millisecond)
	defer restoreRenew()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	const numTopics = 500
	for i := range numTopics {
		require.NoError(t, ob.AcquireTopic(ctx, fmt.Sprintf("orders-%d", i)))
	}

	// One instance, one session row — however many topics it holds.
	var sessionCount int
	require.NoError(t, sharedPool.QueryRow(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s.consumer_sessions", schema)).Scan(&sessionCount))
	assert.Equal(t, 1, sessionCount)

	// Every held topic defers to the session: no per-topic timestamps.
	var overrideCount int
	require.NoError(t, sharedPool.QueryRow(ctx,
		fmt.Sprintf("SELECT count(*) FROM %s.topics WHERE exclusive_consumer_expires_at IS NOT NULL", schema)).Scan(&overrideCount))
	assert.Equal(t, 0, overrideCount)

	snapshotTopicRowVersions := func() map[string]string {
		rows, err := sharedPool.Query(ctx, fmt.Sprintf("SELECT topic, xmin::text FROM %s.topics", schema))
		require.NoError(t, err)
		defer rows.Close()
		versions := make(map[string]string, numTopics)
		for rows.Next() {
			var topic, xmin string
			require.NoError(t, rows.Scan(&topic, &xmin))
			versions[topic] = xmin
		}
		require.NoError(t, rows.Err())
		require.Len(t, versions, numTopics)
		return versions
	}

	var sessionExpiresBefore time.Time
	require.NoError(t, sharedPool.QueryRow(ctx,
		fmt.Sprintf("SELECT expires_at FROM %s.consumer_sessions", schema)).Scan(&sessionExpiresBefore))
	before := snapshotTopicRowVersions()

	// Sleep across several renewal intervals.
	time.Sleep(300 * time.Millisecond)

	// Zero writes landed on any topics row...
	assert.Equal(t, before, snapshotTopicRowVersions(), "topics rows were written during steady-state hold")

	// ...while the centralized session heartbeat kept advancing.
	var sessionExpiresAfter time.Time
	require.NoError(t, sharedPool.QueryRow(ctx,
		fmt.Sprintf("SELECT expires_at FROM %s.consumer_sessions", schema)).Scan(&sessionExpiresAfter))
	assert.True(t, sessionExpiresAfter.After(sessionExpiresBefore),
		"consumer session expires_at did not advance: before=%v after=%v", sessionExpiresBefore, sessionExpiresAfter)
}

// TestExclusiveConsumer_ReacquireBeforeOldWatcherFiresDoesNotReintroduceReleaseOverride
// covers the reacquire race (design Finding 2): cancelling an AcquireTopic
// ctx and immediately re-acquiring the same topic on the same instance must
// not let the old hold's grace-period override land after the fresh acquire,
// where it would silently expire a valid session-deferred lease.
func TestExclusiveConsumer_ReacquireBeforeOldWatcherFiresDoesNotReintroduceReleaseOverride(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	// Lease duration short enough that a stray grace override (now + duration)
	// would lapse mid-test and break ProcessMessages; renewal interval short
	// enough to keep the consumer session fresh despite that short duration.
	restoreDuration := pgoutbox.SetExclusiveLeaseDurationForTest(150 * time.Millisecond)
	defer restoreDuration()
	restoreRenew := pgoutbox.SetExclusiveLeaseRenewIntervalForTest(25 * time.Millisecond)
	defer restoreRenew()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ob.AddFlusher("orders", &noopFlusher{})

	acquireCtx, cancelAcquire := context.WithCancel(ctx)
	require.NoError(t, ob.AcquireTopic(acquireCtx, "orders"))

	// Cancel the first hold and immediately re-acquire on the same instance.
	cancelAcquire()
	require.NoError(t, ob.AcquireTopic(ctx, "orders"))

	// ProcessMessages must keep succeeding well past the point where a stray
	// grace override from the first hold would have expired the lease.
	deadline := time.Now().Add(600 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, err := ob.ProcessMessages(ctx, "orders")
		require.NoError(t, err, "lease lapsed after reacquire — old watcher's override must not survive")
		time.Sleep(50 * time.Millisecond)
	}

	var expiresAt *time.Time
	require.NoError(t, sharedPool.QueryRow(ctx,
		fmt.Sprintf("SELECT exclusive_consumer_expires_at FROM %s.topics WHERE topic = 'orders'", schema)).Scan(&expiresAt))
	assert.Nil(t, expiresAt, "reacquired topic must defer to the consumer session, not carry an override")
}

// TestExclusiveConsumer_HonorsLegacyPerTopicTimestampLease covers the rolling
// upgrade path: an instance running a pre-session version of pgoutbox writes
// its lease as a per-topic exclusive_consumer_expires_at timestamp and has no
// consumer_sessions row. Simulated here with direct SQL, since that lease
// shape can no longer be produced through the API. A session-aware instance
// must honor the legacy lease (the non-NULL timestamp wins the COALESCE over
// the missing session) until it lapses, then take over normally.
func TestExclusiveConsumer_HonorsLegacyPerTopicTimestampLease(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreRetry := pgoutbox.SetExclusiveLeaseRetryIntervalForTest(20 * time.Millisecond)
	defer restoreRetry()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ob.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, ob, "orders", 2)

	legacyHolderID := uuid.New()
	_, err = sharedPool.Exec(ctx, fmt.Sprintf(
		"UPDATE %s.topics SET exclusive_consumer_id = $1, exclusive_consumer_expires_at = now() + interval '500 milliseconds' WHERE topic = 'orders'",
		schema), legacyHolderID)
	require.NoError(t, err)

	// While the legacy lease is valid, the session-aware instance is locked out.
	_, err = ob.ProcessMessages(ctx, "orders")
	require.ErrorIs(t, err, pgoutbox.ErrExclusiveLeaseHeld)

	// Once the legacy timestamp lapses it can take over, exactly as it would
	// have against a pre-session holder that stopped renewing.
	acquireCtx, acquireCancel := context.WithTimeout(ctx, 5*time.Second)
	defer acquireCancel()
	require.NoError(t, ob.AcquireTopic(acquireCtx, "orders"))

	msgs, err := ob.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, msgs, 2)
}
