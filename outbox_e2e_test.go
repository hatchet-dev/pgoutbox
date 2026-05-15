package pgoutbox_test

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/pgoutbox"
	"github.com/hatchet-dev/pgoutbox/internal/harness"
	"github.com/hatchet-dev/pgoutbox/sqlc"
)

const MAX_CONNS int32 = 16

// sharedPool is a single pgxpool.Pool, backed by one postgres container, that
// every test in this file shares. Tests isolate themselves by running against
// a uniquely-generated schema (see uniqueSchema) — the outbox rewrites SQL on
// the wire so the same pool can host any number of parallel-test schemas
// without collision.
var sharedPool *pgxpool.Pool

func TestMain(m *testing.M) {
	os.Exit(runMain(m))
}

func runMain(m *testing.M) int {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	connStr, cleanup, err := harness.Start(ctx)
	if err != nil {
		log.Printf("start postgres: %v", err)
		return 1
	}
	defer cleanup()

	cfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		log.Printf("parse config: %v", err)
		return 1
	}
	// Give parallel tests headroom — each test holds 1-2 conns at peak.
	cfg.MaxConns = MAX_CONNS

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		log.Printf("connect: %v", err)
		return 1
	}
	defer pool.Close()

	sharedPool = pool
	return m.Run()
}

// uniqueSchema returns a Postgres-identifier-safe schema name unique to this
// test invocation. Tests use a fresh schema so they can run in parallel
// against the same database without stepping on each other.
func uniqueSchema(tb testing.TB) string {
	tb.Helper()
	var b [8]byte
	_, err := rand.Read(b[:])
	require.NoError(tb, err)
	return "test_" + hex.EncodeToString(b[:])
}

// captureFlusher is a Flusher that records every message it sees and can be
// configured to fail (to exercise error paths).
type captureFlusher struct {
	mu       sync.Mutex
	received []*sqlc.Message
	failWith error
}

func (c *captureFlusher) Flush(_ context.Context, msgs []*sqlc.Message) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failWith != nil {
		return c.failWith
	}
	c.received = append(c.received, msgs...)
	return nil
}

func (c *captureFlusher) Received() []*sqlc.Message {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*sqlc.Message, len(c.received))
	copy(out, c.received)
	return out
}

func countMessages(t *testing.T, ctx context.Context, schema, topic string) int {
	t.Helper()

	var n int
	query := fmt.Sprintf("SELECT count(*) FROM %s.messages WHERE topic = $1", pgx.Identifier{schema}.Sanitize())
	err := sharedPool.QueryRow(ctx, query, topic).Scan(&n)
	require.NoError(t, err)
	return n
}

func mustPayload(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func TestOutbox_AddAndProcessMessages(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	outbox, err := pgoutbox.NewOutbox(sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	flusher := &captureFlusher{}
	outbox.AddFlusher("orders", flusher)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)

	err = outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]any{"id": 1})},
		{Payload: mustPayload(t, map[string]any{"id": 2})},
		{Payload: mustPayload(t, map[string]any{"id": 3})},
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))

	require.Equal(t, 3, countMessages(t, ctx, schema, "orders"))

	_, err = outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)

	received := flusher.Received()
	require.Len(t, received, 3)
	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"), "flushed messages should be deleted")

	// Calling ProcessMessages again with nothing pending should be a no-op.
	_, err = outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, flusher.Received(), 3, "no additional messages should be delivered")
}

func TestOutbox_TopicRouting(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	outbox, err := pgoutbox.NewOutbox(sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ordersFlusher := &captureFlusher{}
	shipmentsFlusher := &captureFlusher{}

	outbox.AddFlusher("orders", ordersFlusher)
	outbox.AddFlusher("shipments", shipmentsFlusher)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)

	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]string{"order": "a"})},
		{Payload: mustPayload(t, map[string]string{"order": "b"})},
	}))
	require.NoError(t, outbox.AddMessages(ctx, tx, "shipments", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]string{"shipment": "x"})},
	}))
	require.NoError(t, tx.Commit(ctx))

	_, err = outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	_, err = outbox.ProcessMessages(ctx, "shipments")
	require.NoError(t, err)

	assert.Len(t, ordersFlusher.Received(), 2)
	assert.Len(t, shipmentsFlusher.Received(), 1)
	assert.Equal(t, "shipments", shipmentsFlusher.Received()[0].Topic)
}

func TestOutbox_RolledBackTxLeavesNoMessages(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	outbox, err := pgoutbox.NewOutbox(sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	flusher := &captureFlusher{}
	outbox.AddFlusher("orders", flusher)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)

	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
	}))
	require.NoError(t, tx.Rollback(ctx))

	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"))

	_, err = outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Empty(t, flusher.Received())
}

func TestOutbox_NoFlusherRegistered(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	outbox, err := pgoutbox.NewOutbox(sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	_, err = outbox.ProcessMessages(ctx, "unregistered")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unregistered")
}

func TestOutbox_FlusherErrorLeavesMessages(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	outbox, err := pgoutbox.NewOutbox(sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	flusher := &captureFlusher{failWith: fmt.Errorf("destination unavailable")}
	outbox.AddFlusher("orders", flusher)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
		{Payload: mustPayload(t, map[string]int{"id": 2})},
	}))
	require.NoError(t, tx.Commit(ctx))

	_, err = outbox.ProcessMessages(ctx, "orders")
	require.Error(t, err)

	// Failed flush must NOT delete the rows — the outbox guarantee is that
	// messages survive until a Flusher reports success.
	assert.Equal(t, 2, countMessages(t, ctx, schema, "orders"))

	// Once the flusher recovers, the same messages should be delivered.
	flusher.mu.Lock()
	flusher.failWith = nil
	flusher.mu.Unlock()

	_, err = outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, flusher.Received(), 2)
	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"))
}

func TestOutbox_BatchSizeIsRespected(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	outbox, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithBatchSize(1),
	)
	require.NoError(t, err)

	flusher := &captureFlusher{}
	outbox.AddFlusher("orders", flusher)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
		{Payload: mustPayload(t, map[string]int{"id": 2})},
		{Payload: mustPayload(t, map[string]int{"id": 3})},
	}))
	require.NoError(t, tx.Commit(ctx))

	// First call: only 1 of 3 should be flushed.
	processed, err := outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, processed, 1, "ProcessMessages should return exactly batchSize messages")
	assert.Len(t, flusher.Received(), 1)
	assert.Equal(t, 2, countMessages(t, ctx, schema, "orders"))

	// Second call: one more.
	processed, err = outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, processed, 1)
	assert.Len(t, flusher.Received(), 2)
	assert.Equal(t, 1, countMessages(t, ctx, schema, "orders"))

	// Third call: last one.
	processed, err = outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, processed, 1)
	assert.Len(t, flusher.Received(), 3)
	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"))

	// Fourth call: nothing left, no-op.
	processed, err = outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Empty(t, processed)
	assert.Len(t, flusher.Received(), 3)
}

// messagesTableExists returns true if the messages table is present in the
// given schema. Used to verify the auto-migrate behavior.
func messagesTableExists(t *testing.T, ctx context.Context, schema string) bool {
	t.Helper()
	var n int
	err := sharedPool.QueryRow(
		ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_name = 'messages' AND table_schema = $1",
		schema,
	).Scan(&n)
	require.NoError(t, err)
	return n == 1
}

func TestOutbox_WithAutoMigrateFalseSkipsMigration(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	_, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithAutoMigrate(false),
	)
	require.NoError(t, err)

	// The schema itself is not created either — runMigrations creates it,
	// and we skipped runMigrations. The messages table therefore must not
	// exist.
	assert.False(t, messagesTableExists(t, ctx, schema),
		"messages table must not exist when WithAutoMigrate(false) is set and Migrate has not been called")
}

func TestOutbox_ExplicitMigrate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	// Construct the outbox first with auto-migrate disabled — table should
	// not exist yet.
	outbox, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithAutoMigrate(false),
	)
	require.NoError(t, err)
	require.False(t, messagesTableExists(t, ctx, schema))

	// Run migrations explicitly.
	require.NoError(t, pgoutbox.Migrate(ctx, sharedPool, pgoutbox.WithSchema(schema)))
	require.True(t, messagesTableExists(t, ctx, schema))

	// The previously-constructed outbox should now be fully functional.
	flusher := &captureFlusher{}
	outbox.AddFlusher("orders", flusher)

	tx, err := sharedPool.Begin(ctx)
	require.NoError(t, err)
	require.NoError(t, outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{
		{Payload: mustPayload(t, map[string]int{"id": 1})},
		{Payload: mustPayload(t, map[string]int{"id": 2})},
	}))
	require.NoError(t, tx.Commit(ctx))

	processed, err := outbox.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	assert.Len(t, processed, 2)
	assert.Len(t, flusher.Received(), 2)
	assert.Equal(t, 0, countMessages(t, ctx, schema, "orders"))
}

func TestOutbox_InvalidSchemaNameRejected(t *testing.T) {
	t.Parallel()

	// Even with auto-migrate off, the constructor must reject an unsafe
	// schema name — schema flows into SQL on every Add/Process call.
	_, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema("bad-schema; DROP SCHEMA public CASCADE; --"),
		pgoutbox.WithAutoMigrate(false),
	)
	require.Error(t, err)
}

func TestOutbox_TableLandsInConfiguredSchema(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	_, err := pgoutbox.NewOutbox(sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	var schemaName string
	err = sharedPool.QueryRow(
		ctx,
		"SELECT table_schema FROM information_schema.tables WHERE table_name = 'messages' AND table_schema = $1",
		schema,
	).Scan(&schemaName)
	require.NoError(t, err)
	assert.Equal(t, schema, schemaName)
}
