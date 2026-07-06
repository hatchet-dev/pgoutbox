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

// rawInsertMessages inserts n messages directly via SQL, without an outbox
// instance — the insert trigger still registers the topic and bumps its
// activity, but no extra consumer session is created, which keeps
// fair-share-sensitive tests deterministic.
func rawInsertMessages(t *testing.T, ctx context.Context, schema, topic string, n int) {
	t.Helper()
	query := fmt.Sprintf(
		"INSERT INTO %s.messages (topic, payload) SELECT $1::text, '{}'::jsonb FROM generate_series(1, $2::int)",
		pgx.Identifier{schema}.Sanitize(),
	)
	_, err := sharedPool.Exec(ctx, query, topic, n)
	require.NoError(t, err)
}

// backdateTopicActivity sets topics.last_inserted_at = now - age, simulating
// a topic whose last insert happened in the past.
func backdateTopicActivity(t *testing.T, ctx context.Context, schema, topic string, age time.Duration) {
	t.Helper()
	target := time.Now().UTC().Add(-age)
	query := fmt.Sprintf(
		"UPDATE %s.topics SET last_inserted_at = $1 WHERE topic = $2",
		pgx.Identifier{schema}.Sanitize(),
	)
	_, err := sharedPool.Exec(ctx, query, target, topic)
	require.NoError(t, err)
}

// topicActivity reads topics.last_inserted_at for the given topic.
func topicActivity(t *testing.T, ctx context.Context, schema, topic string) time.Time {
	t.Helper()
	query := fmt.Sprintf(
		"SELECT last_inserted_at FROM %s.topics WHERE topic = $1",
		pgx.Identifier{schema}.Sanitize(),
	)
	var ts time.Time
	require.NoError(t, sharedPool.QueryRow(ctx, query, topic).Scan(&ts))
	return ts
}

// hasMaintenanceLease reports whether a maintenance_leases row exists for the
// given topic.
func hasMaintenanceLease(t *testing.T, ctx context.Context, schema, topic string) bool {
	t.Helper()
	query := fmt.Sprintf(
		"SELECT EXISTS(SELECT 1 FROM %s.maintenance_leases WHERE topic = $1)",
		pgx.Identifier{schema}.Sanitize(),
	)
	var exists bool
	require.NoError(t, sharedPool.QueryRow(ctx, query, topic).Scan(&exists))
	return exists
}

// maintenanceLeaseHolders returns lease counts per holder across the schema.
func maintenanceLeaseHolders(t *testing.T, ctx context.Context, schema string) map[string]int {
	t.Helper()
	query := fmt.Sprintf(
		"SELECT holder_id::text, COUNT(*) FROM %s.maintenance_leases GROUP BY 1",
		pgx.Identifier{schema}.Sanitize(),
	)
	rows, err := sharedPool.Query(ctx, query)
	require.NoError(t, err)
	defer rows.Close()
	holders := make(map[string]int)
	for rows.Next() {
		var holder string
		var count int
		require.NoError(t, rows.Scan(&holder, &count))
		holders[holder] = count
	}
	require.NoError(t, rows.Err())
	return holders
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
	// No t.Parallel() — modifies global timing vars shared with other tests.

	// Maintenance leases defer to the holder's consumer session for liveness,
	// so failover is driven by the dead instance's session lapsing: shorten
	// the session heartbeat rather than the pre-session lease timeout.
	restoreDur := pgoutbox.SetExclusiveLeaseDurationForTest(500 * time.Millisecond)
	defer restoreDur()
	restoreRenew := pgoutbox.SetExclusiveLeaseRenewIntervalForTest(100 * time.Millisecond)
	defer restoreRenew()
	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()
	restoreMin := pgoutbox.SetMaintenanceMinIntervalForTest(20 * time.Millisecond)
	defer restoreMin()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	require.NoError(t, pgoutbox.Migrate(ctx, sharedPool, pgoutbox.WithSchema(schema)))

	ctxA, cancelA := context.WithCancel(ctx)

	// Insert and backdate the first batch before starting A so A's maintenance
	// loop finds old messages immediately.
	rawInsertMessages(t, ctx, schema, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	obA, err := pgoutbox.NewOutbox(
		ctxA,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)
	_ = obA

	require.Eventually(t, func() bool {
		return countMessages(t, ctx, schema, "orders") == 0
	}, 10*time.Second, 25*time.Millisecond, "A should clean up the first batch")

	// Insert a second batch that B will need to clean up, then stop A. A's
	// shutdown performs no release writes: its lease frees up purely by its
	// consumer session lapsing.
	rawInsertMessages(t, ctx, schema, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)
	cancelA()

	_, err = pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", time.Second),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return countMessages(t, ctx, schema, "orders") == 0
	}, 10*time.Second, 25*time.Millisecond, "B should take over the lapsed lease and clean up")
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
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()

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

	// "dynamic" used the 100ms default → cleaned up.
	require.Eventually(t, func() bool {
		return countMessages(t, ctx, schema, "dynamic") == 0
	}, 5*time.Second, 25*time.Millisecond, "default expiration should clean up dynamic topics")
	// "known" has a 10s explicit TTL → its messages are not expired yet.
	assert.Equal(t, 2, countMessages(t, ctx, schema, "known"), "per-topic TTL should take precedence over default")
}

func TestExpiry_MultiTopicDifferentTTLs(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()

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

	require.Eventually(t, func() bool {
		return countMessages(t, ctx, schema, "fast") == 0
	}, 5*time.Second, 25*time.Millisecond, "fast-topic messages should be expired")
	assert.Equal(t, 3, countMessages(t, ctx, schema, "slow"), "slow-topic messages should survive")
}

func TestExpiry_DormantTopicReleasesLeaseAndRevives(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()
	restoreSlack := pgoutbox.SetMaintenanceActivitySlackForTest(500 * time.Millisecond)
	defer restoreSlack()
	restoreMin := pgoutbox.SetMaintenanceMinIntervalForTest(20 * time.Millisecond)
	defer restoreMin()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	require.NoError(t, pgoutbox.Migrate(ctx, sharedPool, pgoutbox.WithSchema(schema)))

	rawInsertMessages(t, ctx, schema, "orders", 3)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	ob, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", 100*time.Millisecond),
	)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		return countMessages(t, ctx, schema, "orders") == 0
	}, 10*time.Second, 25*time.Millisecond, "first batch should be cleaned up")

	// Age the topic out of its activity window: the scanner must stop the
	// maintenance loop, run its final cleanup pass, and delete the lease row,
	// leaving the dormant topic with zero goroutines and zero lease state.
	backdateTopicActivity(t, ctx, schema, "orders", time.Hour)

	require.Eventually(t, func() bool {
		return len(pgoutbox.ManagedTopicsForTest(ob)) == 0 && !hasMaintenanceLease(t, ctx, schema, "orders")
	}, 10*time.Second, 25*time.Millisecond, "dormant topic should be unmanaged with no lease row")

	// Reactivate: the insert trigger bumps last_inserted_at, the next scan
	// re-claims the topic, and cleanup resumes.
	rawInsertMessages(t, ctx, schema, "orders", 2)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	require.Eventually(t, func() bool {
		return countMessages(t, ctx, schema, "orders") == 0
	}, 10*time.Second, 25*time.Millisecond, "reactivated topic should be cleaned up again")
}

func TestExpiry_MaintenanceFederatesAcrossInstances(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)

	// Both instances (and so both consumer sessions) exist before any topic
	// does, so the fair-share cap applies from each instance's first claim.
	obA, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema), pgoutbox.WithDefaultExpiration(time.Hour))
	require.NoError(t, err)
	obB, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema), pgoutbox.WithDefaultExpiration(time.Hour))
	require.NoError(t, err)
	_ = obB

	const topics = 40
	for i := range topics {
		insertMessages(t, ctx, obA, fmt.Sprintf("topic-%02d", i), 1)
	}

	require.Eventually(t, func() bool {
		holders := maintenanceLeaseHolders(t, ctx, schema)
		total := 0
		minHeld := topics
		for _, n := range holders {
			total += n
			minHeld = min(minHeld, n)
		}
		return total == topics && len(holders) == 2 && minHeld >= topics/4
	}, 15*time.Second, 100*time.Millisecond, "maintenance leases should be spread across both instances")
}

func TestExpiry_CatchupSweepCleansOrphanedTopic(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()
	restoreSlack := pgoutbox.SetMaintenanceActivitySlackForTest(300 * time.Millisecond)
	defer restoreSlack()
	restoreMin := pgoutbox.SetMaintenanceMinIntervalForTest(20 * time.Millisecond)
	defer restoreMin()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	require.NoError(t, pgoutbox.Migrate(ctx, sharedPool, pgoutbox.WithSchema(schema)))

	// A topic that is dormant from birth: its activity window already lapsed
	// with expired messages still inside (as happens when a TTL is configured
	// only after the topic's last insert was already older than it). Only the
	// catch-up sweep can find it.
	rawInsertMessages(t, ctx, schema, "orders", 3)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)
	backdateTopicActivity(t, ctx, schema, "orders", 2*time.Hour)

	ob, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", 100*time.Millisecond),
	)
	require.NoError(t, err)
	_ = ob

	require.Eventually(t, func() bool {
		return countMessages(t, ctx, schema, "orders") == 0
	}, 10*time.Second, 25*time.Millisecond, "catch-up sweep should find and clean the dormant topic")

	require.Eventually(t, func() bool {
		return !hasMaintenanceLease(t, ctx, schema, "orders")
	}, 10*time.Second, 25*time.Millisecond, "the drained topic should release its lease row")
}

// TestExpiry_DefaultTopicsFairShareIgnoresIneligibleSessions: every NewOutbox
// registers a live consumer session, but only instances configured with a
// default expiration can see default-TTL topics as maintenance candidates.
// The fair-share denominator for that class must count only the eligible
// sessions — dividing by all live sessions would cap the sole maintainer at
// half the backlog and let the rest trickle in at one topic per scan tick.
func TestExpiry_DefaultTopicsFairShareIgnoresIneligibleSessions(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()
	restoreMin := pgoutbox.SetMaintenanceMinIntervalForTest(20 * time.Millisecond)
	defer restoreMin()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	require.NoError(t, pgoutbox.Migrate(ctx, sharedPool, pgoutbox.WithSchema(schema)))

	const numTopics = 80
	for i := range numTopics {
		rawInsertMessages(t, ctx, schema, fmt.Sprintf("orders-%d", i), 1)
	}

	// A process-only instance: contributes a live session but has no default
	// expiration, so it can never claim any of these topics.
	_, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	// The sole eligible maintainer: its fair share of the default-TTL class
	// is the entire backlog. Under a live-session denominator it would claim
	// half up front and the rest one per tick — far past this deadline.
	_, err = pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema),
		pgoutbox.WithDefaultExpiration(time.Hour))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		holders := maintenanceLeaseHolders(t, ctx, schema)
		total := 0
		for _, n := range holders {
			total += n
		}
		return len(holders) == 1 && total == numTopics
	}, 2500*time.Millisecond, 50*time.Millisecond,
		"the sole default-expiration maintainer should claim the whole backlog within a few scan ticks")
}

// TestExpiry_CatchupSweepDeletesStaleConsumerSessions: instance ids are
// per-NewOutbox, so every restart strands one consumer_sessions row; the
// catch-up sweep garbage-collects rows expired past the retention window so
// the table stays at O(live instances). Retention is kept comfortably above
// lease timeouts here because deleting a session downgrades its leases from
// "expired session, claim immediately" to the acquired_at staleness fallback.
func TestExpiry_CatchupSweepDeletesStaleConsumerSessions(t *testing.T) {
	// No t.Parallel() — this test finishes quickly, so its deferred restores
	// of the scan/catch-up globals would land mid-run of the long parallel
	// tests that also depend on them (e.g. PreSessionHolderIsRespected).

	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()
	restoreCatchup := pgoutbox.SetMaintenanceCatchupIntervalForTest(100 * time.Millisecond)
	defer restoreCatchup()
	restoreRetention := pgoutbox.SetConsumerSessionRetentionForTest(30 * time.Second)
	defer restoreRetention()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	_, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	// A session stranded by a dead instance: expired an hour ago, well past
	// retention.
	_, err = sharedPool.Exec(ctx, fmt.Sprintf(
		"INSERT INTO %s.consumer_sessions (consumer_id, expires_at) VALUES (gen_random_uuid(), now() - interval '1 hour')",
		pgx.Identifier{schema}.Sanitize()))
	require.NoError(t, err)

	countSessions := func() int {
		var n int
		query := fmt.Sprintf("SELECT count(*) FROM %s.consumer_sessions", pgx.Identifier{schema}.Sanitize())
		require.NoError(t, sharedPool.QueryRow(ctx, query).Scan(&n))
		return n
	}

	require.Eventually(t, func() bool {
		return countSessions() == 1
	}, 10*time.Second, 25*time.Millisecond, "the sweep should delete the stale session and keep this instance's live one")
}

func TestExpiry_PreSessionHolderIsRespected(t *testing.T) {
	// No t.Parallel() — modifies global timing vars shared with other tests.

	restoreScan := pgoutbox.SetTopicScanIntervalForTest(100 * time.Millisecond)
	defer restoreScan()
	restoreTimeout := pgoutbox.SetMaintenanceLeaseTimeoutForTest(1500 * time.Millisecond)
	defer restoreTimeout()
	restoreMin := pgoutbox.SetMaintenanceMinIntervalForTest(20 * time.Millisecond)
	defer restoreMin()
	// Parallel tests shrink the activity slack globally, which can age this
	// topic out of its window while the foreign lease is still being honored.
	// A short catch-up interval keeps the topic a candidate throughout (it
	// holds messages), so the takeover happens as soon as the lease is stale.
	restoreCatchup := pgoutbox.SetMaintenanceCatchupIntervalForTest(200 * time.Millisecond)
	defer restoreCatchup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	require.NoError(t, pgoutbox.Migrate(ctx, sharedPool, pgoutbox.WithSchema(schema)))

	rawInsertMessages(t, ctx, schema, "orders", 3)
	backdateMessages(t, ctx, schema, "orders", 2*time.Hour)

	// A lease held by an instance with no consumer session row, exactly as a
	// pre-session version of pgoutbox writes it. It must be honored until
	// acquired_at goes stale, and be claimable afterward.
	query := fmt.Sprintf(
		"INSERT INTO %s.maintenance_leases (topic, holder_id) VALUES ($1, gen_random_uuid())",
		pgx.Identifier{schema}.Sanitize(),
	)
	_, err := sharedPool.Exec(ctx, query, "orders")
	require.NoError(t, err)

	_, err = pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithTopicExpiration("orders", 100*time.Millisecond),
	)
	require.NoError(t, err)

	time.Sleep(500 * time.Millisecond)
	assert.Equal(t, 3, countMessages(t, ctx, schema, "orders"), "a fresh pre-session lease must not be stolen")

	require.Eventually(t, func() bool {
		return countMessages(t, ctx, schema, "orders") == 0
	}, 10*time.Second, 25*time.Millisecond, "the lease should be taken over once acquired_at goes stale")
}

// TestExpiry_ProcessingBumpsTopicActivity: ProcessMessages holds the topic
// row lock for its whole transaction, so the insert trigger's SKIP LOCKED
// activity bump skips continuously-processed topics — potentially every
// window, leaving a busy topic looking dormant. The flush path compensates
// by refreshing the activity stamp itself when it processed messages, gated
// to the trigger's 30s granularity.
func TestExpiry_ProcessingBumpsTopicActivity(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	ob, err := pgoutbox.NewOutbox(ctx, sharedPool, pgoutbox.WithSchema(schema))
	require.NoError(t, err)

	ob.AddFlusher("orders", &noopFlusher{})
	insertMessages(t, ctx, ob, "orders", 2)

	// Simulate a topic whose trigger bumps were all starved: activity is
	// stale even though messages are flowing.
	backdateTopicActivity(t, ctx, schema, "orders", time.Hour)

	msgs, err := ob.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	require.Len(t, msgs, 2)

	bumped := topicActivity(t, ctx, schema, "orders")
	assert.WithinDuration(t, time.Now(), bumped, time.Minute, "processing must refresh stale topic activity")

	// Inside the 30s window neither the trigger nor a second flush may bump
	// again — the gate is what keeps this from becoming per-flush row churn.
	insertMessages(t, ctx, ob, "orders", 1)
	msgs, err = ob.ProcessMessages(ctx, "orders")
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, bumped, topicActivity(t, ctx, schema, "orders"), "the flush-path bump must respect the 30s gate")
}

func TestExpiry_TriggerActivityBumpIsGated(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	schema := uniqueSchema(t)
	require.NoError(t, pgoutbox.Migrate(ctx, sharedPool, pgoutbox.WithSchema(schema)))

	rawInsertMessages(t, ctx, schema, "orders", 1)
	first := topicActivity(t, ctx, schema, "orders")

	rawInsertMessages(t, ctx, schema, "orders", 1)
	second := topicActivity(t, ctx, schema, "orders")
	assert.True(t, second.Equal(first), "an insert within the 30s gate window must not bump last_inserted_at")

	backdateTopicActivity(t, ctx, schema, "orders", time.Hour)
	rawInsertMessages(t, ctx, schema, "orders", 1)
	bumped := topicActivity(t, ctx, schema, "orders")
	assert.WithinDuration(t, time.Now(), bumped, time.Minute, "an insert past the gate window must bump last_inserted_at")
}
