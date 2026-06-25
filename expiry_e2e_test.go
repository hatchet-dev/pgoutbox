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

// setupOutbox creates an outbox with no expiration configured — useful for
// inserting test messages before creating the outbox with expiration so that
// the maintenance loop finds old messages on its first pass rather than
// sleeping the full idle interval.
func setupOutbox(t *testing.T, ctx context.Context, schema string) pgoutbox.Outbox {
	t.Helper()
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)
	return ob
}

func TestExpiry_ExpiredMessagesAreDeleted(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	setup := setupOutbox(t, ctx, schema)

	insertMessages(t, ctx, setup, "orders", 3)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	// Create the outbox with expiration after messages exist so the maintenance
	// loop finds old messages on its first pass and deletes them immediately.
	ob, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)
	_ = ob

	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"), "expired messages should be deleted")
}

func TestExpiry_UnexpiredMessagesArePreserved(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	setup := setupOutbox(t, ctx, schema)

	insertMessages(t, ctx, setup, "orders", 3)

	ob, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", 10*time.Second),
	)
	require.NoError(t, err)
	_ = ob

	time.Sleep(200 * time.Millisecond)

	assert.Equal(t, 3, countMessages(t, ctx, schema, "orders"), "unexpired messages must not be deleted")
}

func TestExpiry_PartialExpiry(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	setup := setupOutbox(t, ctx, schema)

	// Insert the "old" batch and immediately backdate it.
	insertMessages(t, ctx, setup, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	// Insert a fresh batch — these have current timestamps and won't expire.
	insertMessages(t, ctx, setup, "orders", 2)

	ob, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)
	_ = ob

	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 2, countMessages(t, ctx, schema, "orders"), "only expired messages should be deleted")
}

func TestExpiry_LeaseExclusion(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	setup := setupOutbox(t, ctx, schema)

	insertMessages(t, ctx, setup, "orders", 4)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	// Create both instances after messages are in place so both maintenance
	// loops find old messages on their first pass and race for the lease.
	obA, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	obB, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	_ = obA
	_ = obB

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
	setup := setupOutbox(t, ctx, schema)

	ctxA, cancelA := context.WithCancel(ctx)

	// Insert and backdate the first batch before starting A so A's maintenance
	// loop finds old messages immediately.
	insertMessages(t, ctx, setup, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	obA, err := pgoutbox.NewOutbox(
		ctxA,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)
	_ = obA

	time.Sleep(300 * time.Millisecond)
	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"), "A should clean up the first batch")

	// Insert a second batch that B will need to clean up.
	insertMessages(t, ctx, setup, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	// Stop A — it no longer renews the lease.
	cancelA()

	// Wait for A's lease to expire, then start B.
	time.Sleep(300 * time.Millisecond)

	_, err = pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	time.Sleep(300 * time.Millisecond)

	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"), "B should claim the expired lease and clean up")
}

func TestExpiry_NoExpirationsIsNoop(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	// Must not panic when no expirations are configured.
	_, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)
}

func TestExpiry_DefaultExpiration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	setup := setupOutbox(t, ctx, schema)

	// "known" has a per-topic override; "dynamic" is only registered by the
	// trigger and relies entirely on the default expiration.
	insertMessages(t, ctx, setup, "known", 2)
	insertMessages(t, ctx, setup, "dynamic", 2)
	// Both batches are old enough to exceed the default TTL.
	backdateMessages(t, ctx, schema, "known", 500*time.Millisecond)
	backdateMessages(t, ctx, schema, "dynamic", 500*time.Millisecond)

	ob, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("known", 10*time.Second), // explicit — should NOT expire yet
		pgoutbox.WithDefaultExpiration(100*time.Millisecond),
	)
	require.NoError(t, err)
	_ = ob

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
	setup := setupOutbox(t, ctx, schema)

	insertMessages(t, ctx, setup, "fast", 3)
	insertMessages(t, ctx, setup, "slow", 3)

	// Backdate by 500ms: exceeds "fast" (100ms) but not "slow" (10s).
	backdateMessages(t, ctx, schema, "fast", 500*time.Millisecond)
	backdateMessages(t, ctx, schema, "slow", 500*time.Millisecond)

	ob, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("fast", 100*time.Millisecond),
		pgoutbox.WithTopicExpiration("slow", 10*time.Second),
	)
	require.NoError(t, err)
	_ = ob

	time.Sleep(400 * time.Millisecond)

	assert.Equal(t, 0, countMessages(t, ctx, schema, "fast"), "fast-topic messages should be expired")
	assert.Equal(t, 3, countMessages(t, ctx, schema, "slow"), "slow-topic messages should survive")
}
