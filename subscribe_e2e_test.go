package pgoutbox_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/pgoutbox"
)

// newTestPubSub returns a PGPubSub on the shared pool, isolated on its own
// NOTIFY channel so parallel tests don't wake each other up. Schema names are
// identifier-safe, so they double as channel names.
func newTestPubSub(t *testing.T, ctx context.Context, channel string) pgoutbox.PubSub {
	t.Helper()
	ps, err := pgoutbox.NewPGPubSub(ctx, sharedPool, pgoutbox.WithNotifyChannel(channel))
	require.NoError(t, err)
	return ps
}

func TestPGPubSub_PubAndSub(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ps := newTestPubSub(t, ctx, uniqueSchema(t))

	orders, err := ps.Sub(ctx, "orders")
	require.NoError(t, err)
	shipments, err := ps.Sub(ctx, "shipments")
	require.NoError(t, err)

	require.NoError(t, ps.Pub(ctx, "orders", []byte(`{"hello":"world"}`)))

	select {
	case msg := <-orders:
		require.NotNil(t, msg)
		assert.Equal(t, "orders", msg.Topic)
		assert.JSONEq(t, `{"hello":"world"}`, string(msg.Payload))
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for published message")
	}

	// The shipments subscriber must not have seen the orders message.
	select {
	case msg := <-shipments:
		t.Fatalf("shipments subscriber received message for topic %q", msg.Topic)
	default:
	}
}

func TestPGPubSub_SubChannelClosesOnCtxCancel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	ps := newTestPubSub(t, ctx, uniqueSchema(t))

	subCtx, subCancel := context.WithCancel(ctx)
	ch, err := ps.Sub(subCtx, "orders")
	require.NoError(t, err)

	subCancel()

	select {
	case _, ok := <-ch:
		assert.False(t, ok, "channel should be closed, not carrying a message")
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for subscription channel to close")
	}
}

// TestOutbox_SubscribeWakesOnNotification verifies the notification fast
// path: with a poll interval far longer than the test, messages still flush
// promptly after commit — and, because the PGPubSub publishes inside the
// AddMessages transaction, not before it.
func TestOutbox_SubscribeWakesOnNotification(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ps := newTestPubSub(t, ctx, schema)

	outbox, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema), pgoutbox.WithPubSub(ps))
	require.NoError(t, err)

	flusher := &captureFlusher{}
	outbox.AddFlusher("orders", flusher)

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	done := make(chan error, 1)
	go func() {
		done <- outbox.Subscribe(subCtx, "orders", pgoutbox.WithPollInterval(time.Minute))
	}()

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
		{Payload: mustPayload(t, map[string]int{"id": 2})},
		{Payload: mustPayload(t, map[string]int{"id": 3})},
	}))

	// The transaction is still open: no notification has been delivered and
	// the messages are invisible, so nothing may flush yet.
	time.Sleep(1500 * time.Millisecond)
	assert.Empty(t, flusher.Received(), "nothing should flush before the staging tx commits")

	require.NoError(t, tx.Commit(ctx))

	// Poll interval is one minute, so delivery within a few seconds proves
	// the pg_notify wake-up worked. The flusher records messages before the
	// processing tx commits, so also wait for the delete to land.
	require.Eventually(t, func() bool {
		return len(flusher.Received()) == 3 && countMessages(t, ctx, schema, "orders") == 0
	}, 15*time.Second, 50*time.Millisecond, "subscriber should flush promptly on notification")

	subCancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("Subscribe did not return after ctx cancellation")
	}
}

// TestOutbox_SubscribePollsWithoutPubSub verifies the polling fallback: with
// no PubSub configured, Subscribe still picks up messages within a poll
// interval.
func TestOutbox_SubscribePollsWithoutPubSub(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	outbox, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	flusher := &captureFlusher{}
	outbox.AddFlusher("orders", flusher)

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()

	done := make(chan error, 1)
	go func() {
		done <- outbox.Subscribe(subCtx, "orders", pgoutbox.WithPollInterval(100*time.Millisecond))
	}()

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
		{Payload: mustPayload(t, map[string]int{"id": 2})},
	}))
	require.NoError(t, tx.Commit(ctx))

	// As above, wait for both the flush and the committed delete.
	require.Eventually(t, func() bool {
		return len(flusher.Received()) == 2 && countMessages(t, ctx, schema, "orders") == 0
	}, 15*time.Second, 50*time.Millisecond, "subscriber should flush via polling")

	subCancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("Subscribe did not return after ctx cancellation")
	}
}

// addTestMessages stages and commits n messages for topic through ob.
func addTestMessages(t *testing.T, ctx context.Context, ob pgoutbox.Outbox, topic string, n int) {
	t.Helper()

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(ctx)

	msgs := make([]pgoutbox.MessageOpts, n)
	for i := range msgs {
		msgs[i] = pgoutbox.MessageOpts{Payload: mustPayload(t, map[string]int{"id": i})}
	}
	require.NoError(t, ob.AddMessages(ctx, tx, topic, msgs))
	require.NoError(t, tx.Commit(ctx))
}

// TestOutbox_SubscribeExclusiveFailover verifies the WithExclusive failover
// group: instance A acquires the lease and drains; standby B blocks behind
// the lease and processes nothing; when A's Subscribe exits, its
// release-on-return hands the lease to B, which takes over promptly (well
// within the poll interval, via the acquire retry loop plus its post-acquire
// drain pass).
func TestOutbox_SubscribeExclusiveFailover(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ps := newTestPubSub(t, ctx, schema)

	obA, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema), pgoutbox.WithPubSub(ps))
	require.NoError(t, err)
	obB, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema), pgoutbox.WithPubSub(ps))
	require.NoError(t, err)

	flusherA := &captureFlusher{}
	flusherB := &captureFlusher{}
	obA.AddFlusher("orders", flusherA)
	obB.AddFlusher("orders", flusherB)

	ctxA, cancelA := context.WithCancel(ctx)
	defer cancelA()
	doneA := make(chan error, 1)
	go func() {
		doneA <- obA.Subscribe(ctxA, "orders", pgoutbox.WithExclusive(), pgoutbox.WithPollInterval(time.Minute))
	}()

	addTestMessages(t, ctx, obA, "orders", 2)
	require.Eventually(t, func() bool {
		return len(flusherA.Received()) == 2 && countMessages(t, ctx, schema, "orders") == 0
	}, 15*time.Second, 50*time.Millisecond, "A should acquire the lease and drain the first batch")

	// B lines up as the standby: its Subscribe blocks inside AcquireTopic
	// while A holds the lease.
	ctxB, cancelB := context.WithCancel(ctx)
	defer cancelB()
	doneB := make(chan error, 1)
	go func() {
		doneB <- obB.Subscribe(ctxB, "orders", pgoutbox.WithExclusive(), pgoutbox.WithPollInterval(time.Minute))
	}()

	addTestMessages(t, ctx, obA, "orders", 2)
	require.Eventually(t, func() bool {
		return len(flusherA.Received()) == 4 && countMessages(t, ctx, schema, "orders") == 0
	}, 15*time.Second, 50*time.Millisecond, "A should keep draining while it holds the lease")
	assert.Empty(t, flusherB.Received(), "standby B must not process while A holds the lease")

	// A steps down. Its release-on-return expires the lease immediately, so
	// B's blocked AcquireTopic wins on its next retry.
	cancelA()
	select {
	case err := <-doneA:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("A's Subscribe did not return after cancellation")
	}

	addTestMessages(t, ctx, obB, "orders", 2)
	require.Eventually(t, func() bool {
		return len(flusherB.Received()) == 2 && countMessages(t, ctx, schema, "orders") == 0
	}, 15*time.Second, 50*time.Millisecond, "B should take over after A releases the lease")
	assert.Len(t, flusherA.Received(), 4, "A must not process after stepping down")

	cancelB()
	select {
	case err := <-doneB:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("B's Subscribe did not return after cancellation")
	}
}

// TestOutbox_SubscribeExclusiveReacquiresLostLease verifies the self-heal
// path: when the lease is lost mid-subscribe, the next processing pass fails
// with ErrExclusiveLeaseRequired and Subscribe re-acquires on its own instead
// of spinning on errors.
func TestOutbox_SubscribeExclusiveReacquiresLostLease(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ps := newTestPubSub(t, ctx, schema)

	outbox, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema), pgoutbox.WithPubSub(ps))
	require.NoError(t, err)

	flusher := &captureFlusher{}
	outbox.AddFlusher("orders", flusher)

	subCtx, subCancel := context.WithCancel(ctx)
	defer subCancel()
	done := make(chan error, 1)
	go func() {
		done <- outbox.Subscribe(subCtx, "orders", pgoutbox.WithExclusive(), pgoutbox.WithPollInterval(time.Minute))
	}()

	addTestMessages(t, ctx, outbox, "orders", 1)
	require.Eventually(t, func() bool {
		return len(flusher.Received()) == 1
	}, 15*time.Second, 50*time.Millisecond, "subscriber should drain while holding the lease")

	// Expire the lease behind the subscriber's back. The wake-up for the
	// messages below then fails its pass and must trigger a re-acquire.
	_, err = sharedPool.Exec(ctx, fmt.Sprintf(
		"UPDATE %s.topics SET exclusive_consumer_expires_at = now() - interval '1 second' WHERE topic = $1",
		pgx.Identifier{schema}.Sanitize()), "orders")
	require.NoError(t, err)

	addTestMessages(t, ctx, outbox, "orders", 2)
	require.Eventually(t, func() bool {
		return len(flusher.Received()) == 3 && countMessages(t, ctx, schema, "orders") == 0
	}, 15*time.Second, 50*time.Millisecond, "subscriber should re-acquire the lost lease and keep draining")

	subCancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("Subscribe did not return after cancellation")
	}
}

func TestOutbox_SubscribeRequiresFlusher(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	outbox, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	err = outbox.Subscribe(ctx, "unregistered")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unregistered")
	assert.False(t, errors.Is(err, context.Canceled))
}
