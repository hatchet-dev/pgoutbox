package pgoutbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	var notifier pgoutbox.Notifier
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
		{Payload: mustPayload(t, map[string]int{"id": 2})},
		{Payload: mustPayload(t, map[string]int{"id": 3})},
	}, pgoutbox.WithNotifier(&notifier)))

	// The transaction is still open: no notification has been delivered and
	// the messages are invisible, so nothing may flush yet.
	time.Sleep(1500 * time.Millisecond)
	assert.Empty(t, flusher.Received(), "nothing should flush before the staging tx commits")

	require.NoError(t, tx.Commit(ctx))
	// The PGPubSub implements TxPublisher, so the notification already rode
	// the transaction and this is a no-op — invoked anyway to model the
	// post-commit contract callers should follow.
	notifier.Notify(ctx)

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
	var notifier pgoutbox.Notifier
	require.NoError(t, ob.AddMessages(ctx, tx, topic, msgs, pgoutbox.WithNotifier(&notifier)))
	require.NoError(t, tx.Commit(ctx))
	notifier.Notify(ctx)
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

// fakePubSub is an in-memory PubSub that does not implement TxPublisher, so
// an outbox using it must defer its new-message notification to a Notifier
// passed to AddMessages via WithNotifier.
type fakePubSub struct {
	mu   sync.Mutex
	pubs []string
	subs map[string][]chan *pgoutbox.PubSubMessage
}

func newFakePubSub() *fakePubSub {
	return &fakePubSub{subs: make(map[string][]chan *pgoutbox.PubSubMessage)}
}

func (f *fakePubSub) Pub(_ context.Context, topic string, payload []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pubs = append(f.pubs, topic)
	for _, ch := range f.subs[topic] {
		select {
		case ch <- &pgoutbox.PubSubMessage{Topic: topic, Payload: payload}:
		default:
		}
	}
	return nil
}

func (f *fakePubSub) Sub(_ context.Context, topic string) (<-chan *pgoutbox.PubSubMessage, error) {
	ch := make(chan *pgoutbox.PubSubMessage, 16)
	f.mu.Lock()
	f.subs[topic] = append(f.subs[topic], ch)
	f.mu.Unlock()
	return ch, nil
}

func (f *fakePubSub) pubCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pubs)
}

func (f *fakePubSub) subCount(topic string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs[topic])
}

// fakeTxPubSub additionally implements TxPublisher, recording in-tx publishes
// so tests can assert the Notifier does not publish a second time.
type fakeTxPubSub struct {
	fakePubSub
	inTx []string // guarded by fakePubSub.mu
}

func (f *fakeTxPubSub) PubInTx(_ context.Context, _ pgx.Tx, topic string, _ []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.inTx = append(f.inTx, topic)
	return nil
}

func (f *fakeTxPubSub) inTxCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.inTx)
}

// TestOutbox_GenericPubSubNotifiesViaNotifier verifies the non-TxPublisher
// path: AddMessages publishes nothing on its own — not at insert time, not at
// commit — and the Notifier passed via WithNotifier accumulates one
// notification per call, carried past the transaction and fired after commit
// to wake a Subscribe consumer.
func TestOutbox_GenericPubSubNotifiesViaNotifier(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ps := newFakePubSub()

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

	// Wait until the subscriber is registered, then give its initial
	// (empty-topic) processing pass a moment to finish, so the flush below can
	// only come from the notifier's wake-up — the poll interval is a minute.
	require.Eventually(t, func() bool {
		return ps.subCount("orders") == 1
	}, 15*time.Second, 10*time.Millisecond, "Subscribe should register with the PubSub")
	time.Sleep(250 * time.Millisecond)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	var notifier pgoutbox.Notifier
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
		{Payload: mustPayload(t, map[string]int{"id": 2})},
	}, pgoutbox.WithNotifier(&notifier)))
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 3})},
	}, pgoutbox.WithNotifier(&notifier)))
	assert.Zero(t, ps.pubCount(), "AddMessages must not publish at insert time")

	require.NoError(t, tx.Commit(ctx))
	assert.Zero(t, ps.pubCount(), "commit alone must not publish for a generic PubSub")

	notifier.Notify(ctx)
	assert.Equal(t, 2, ps.pubCount(), "Notify should publish once per accumulated AddMessages call")

	require.Eventually(t, func() bool {
		return len(flusher.Received()) == 3 && countMessages(t, ctx, schema, "orders") == 0
	}, 15*time.Second, 50*time.Millisecond, "subscriber should flush promptly on the notifier's wake-up")

	subCancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(15 * time.Second):
		t.Fatal("Subscribe did not return after ctx cancellation")
	}
}

// TestOutbox_TxPublisherNotifierIsNoop verifies the TxPublisher path decided
// at construction: AddMessages publishes on the caller's transaction and adds
// nothing to the Notifier, so Notify does not publish a second time.
func TestOutbox_TxPublisherNotifierIsNoop(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ps := &fakeTxPubSub{fakePubSub: fakePubSub{subs: make(map[string][]chan *pgoutbox.PubSubMessage)}}

	outbox, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema), pgoutbox.WithPubSub(ps))
	require.NoError(t, err)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	var notifier pgoutbox.Notifier
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
	}, pgoutbox.WithNotifier(&notifier)))
	require.Equal(t, 1, ps.inTxCount(), "the notification should ride the caller's transaction")
	require.NoError(t, tx.Commit(ctx))

	notifier.Notify(ctx)
	assert.Zero(t, ps.pubCount(), "Notify must not publish a second notification")
}
