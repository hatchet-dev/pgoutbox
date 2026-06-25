package pgoutbox_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/pgoutbox"
)

// backdateMessages sets inserted_at = now - age for all messages in the
// given topic/schema, simulating messages that arrived in the past.
func backdateMessages(t *testing.T, ctx context.Context, schema, topic string, age time.Duration) {
	t.Helper()
	target := time.Now().UTC().Add(-age)
	query := fmt.Sprintf(
		"UPDATE %s.messages SET inserted_at = $1 WHERE topic = $2",
		pgx.Identifier{schema}.Sanitize(),
	)
	_, err := sharedPool.Exec(ctx, query, target, topic)
	require.NoError(t, err)
}

// insertMessages adds n messages with dummy payload to the given topic inside a
// committed transaction.
func insertMessages(t *testing.T, ctx context.Context, ob pgoutbox.Outbox, topic string, n int) {
	t.Helper()
	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	msgs := make([]pgoutbox.MessageOpts, n)
	for i := range msgs {
		msgs[i] = pgoutbox.MessageOpts{Payload: mustPayload(t, map[string]int{"i": i})}
	}
	require.NoError(t, ob.AddMessages(ctx, tx, topic, msgs))
	require.NoError(t, tx.Commit(ctx))
}

func TestExpiry_ExpiredMessagesAreDeleted(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	insertMessages(t, ctx, ob, "orders", 3)
	// Backdate so messages are already past their TTL.
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	ob.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"), "expired messages should be deleted")
}

func TestExpiry_UnexpiredMessagesArePreserved(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", 10*time.Second),
	)
	require.NoError(t, err)

	insertMessages(t, ctx, ob, "orders", 3)

	ob.Start(ctx)
	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, 3, countMessages(t, ctx, schema, "orders"), "unexpired messages must not be deleted")
}

func TestExpiry_PartialExpiry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	// Insert the "old" batch and immediately backdate it.
	insertMessages(t, ctx, ob, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	// Insert a fresh batch — these have current timestamps and won't expire.
	insertMessages(t, ctx, ob, "orders", 2)

	ob.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 2, countMessages(t, ctx, schema, "orders"), "only expired messages should be deleted")
}

func TestExpiry_LeaseExclusion(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	obA, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	// obB re-uses the same pool and schema — simulates two app instances.
	obB, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	insertMessages(t, ctx, obA, "orders", 4)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	obA.Start(ctx)
	obB.Start(ctx)
	time.Sleep(500 * time.Millisecond)

	// Exactly 0 messages should remain — the lease ensures one instance won
	// and there should be no panics or double-processing errors.
	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"))
}

func TestExpiry_LeaseHandoff(t *testing.T) {
	t.Parallel()

	// Use a very short lease timeout so the handoff happens in milliseconds.
	restore := pgoutbox.SetMaintenanceLeaseTimeoutForTest(200 * time.Millisecond)
	defer restore()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	ctxA, cancelA := context.WithCancel(ctx)

	obA, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	// Insert and backdate a first batch; let A clean it up.
	insertMessages(t, ctx, obA, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	obA.Start(ctxA)
	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"), "A should clean up the first batch")

	// Insert a second batch that B will need to clean up.
	insertMessages(t, ctx, obA, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	// Stop A — it no longer renews the lease.
	cancelA()

	// Wait for the lease to expire, then start B.
	time.Sleep(300 * time.Millisecond)

	obB, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	obB.Start(ctx)
	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"), "B should claim the expired lease and clean up")
}

func TestExpiry_StartIsNoopWithoutExpirations(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	// Must not panic.
	ob.Start(ctx)
}

func TestExpiry_DefaultExpiration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	// "known" has a per-topic override; "dynamic" is only registered by the
	// trigger and relies entirely on the default expiration.
	ob, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("known", 10*time.Second), // explicit — should NOT expire yet
		pgoutbox.WithDefaultExpiration(100*time.Millisecond),
	)
	require.NoError(t, err)

	insertMessages(t, ctx, ob, "known", 2)
	insertMessages(t, ctx, ob, "dynamic", 2)
	// Both batches are old enough to exceed the default TTL.
	backdateMessages(t, ctx, schema, "known", 500*time.Millisecond)
	backdateMessages(t, ctx, schema, "dynamic", 500*time.Millisecond)

	ob.Start(ctx)
	time.Sleep(400 * time.Millisecond)

	// "dynamic" used the 100ms default → cleaned up.
	assert.Equal(t, 0, countMessages(t, ctx, schema, "dynamic"), "default expiration should clean up dynamic topics")
	// "known" has a 10s explicit TTL → messages are only 500ms old, not expired.
	assert.Equal(t, 2, countMessages(t, ctx, schema, "known"), "per-topic TTL should take precedence over default")
}

func TestExpiry_MultiTopicDifferentTTLs(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("fast", 100*time.Millisecond),
		pgoutbox.WithTopicExpiration("slow", 10*time.Second),
	)
	require.NoError(t, err)

	insertMessages(t, ctx, ob, "fast", 3)
	insertMessages(t, ctx, ob, "slow", 3)

	// Backdate both — "fast" TTL has elapsed, "slow" has not yet (we won't
	// wait 10s in a unit test; instead we check that the goroutine only
	// deletes what's expired according to its configured TTL).
	// Backdate by 500ms: exceeds "fast" (100ms) but not "slow" (10s).
	backdateMessages(t, ctx, schema, "fast", 500*time.Millisecond)
	backdateMessages(t, ctx, schema, "slow", 500*time.Millisecond)

	ob.Start(ctx)
	time.Sleep(400 * time.Millisecond)

	assert.Equal(t, 0, countMessages(t, ctx, schema, "fast"), "fast-topic messages should be expired")
	assert.Equal(t, 3, countMessages(t, ctx, schema, "slow"), "slow-topic messages should survive")
}
