package pgoutbox

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/pgoutbox/internal/dbwrap"
	"github.com/hatchet-dev/pgoutbox/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// FlushContext is the context passed to Flusher.Flush. It embeds
// context.Context and exposes the transaction that ProcessMessages uses to
// lock and delete messages. Callers that want their writes to commit
// atomically with the outbox delete can enlist in that transaction via Tx().
type FlushContext interface {
	context.Context
	Tx() pgx.Tx
}

type flushContext struct {
	context.Context
	tx pgx.Tx
}

func (f *flushContext) Tx() pgx.Tx { return f.tx }

type Flusher interface {
	Flush(ctx FlushContext, msgs []*sqlc.Message) error
}

type MessageOpts struct {
	Payload []byte
}

type Outbox interface {
	AddFlusher(topic string, flusher Flusher)

	AddMessages(ctx context.Context, tx pgx.Tx, topic string, msgs []MessageOpts) error

	// ProcessMessages grabs a batch of messages for the given topic, flushes them using the registered Flusher for that
	// topic, and deletes them from the outbox if the flush is successful. If the topic has an active exclusive consumer,
	// the calling instance must hold the exclusive lease (via AcquireTopic) or an error is returned.
	ProcessMessages(ctx context.Context, topic string, opts ...ProcessOpt) ([]*sqlc.Message, error)

	// AcquireTopic blocks until this instance holds the exclusive processing lease
	// for the named topic, then returns. A background goroutine automatically renews
	// the lease until ctx is cancelled or ReleaseTopic is called, at which point the
	// lease expires naturally and another instance can take over. AcquireTopic must
	// be called before ProcessMessages for any topic that has an active exclusive
	// consumer.
	AcquireTopic(ctx context.Context, topic string) error

	// ReleaseTopic stops renewing and immediately expires the exclusive lease
	// this instance holds for topic, letting another instance acquire it right
	// away instead of waiting out the lease duration. It is a no-op if this
	// instance does not currently hold the lease. As with a naturally expired
	// lease, a subsequent ProcessMessages call still requires an explicit
	// AcquireTopic first.
	ReleaseTopic(ctx context.Context, topic string) error
}

// ErrExclusiveLeaseHeld is returned by ProcessMessages when another outbox
// instance currently holds a valid exclusive lease for the topic.
var ErrExclusiveLeaseHeld = errors.New("exclusive lease held by another instance")

// defaultBatchSize is the number of messages ProcessMessages will pull per
// call when the caller has not specified WithBatchSize.
const defaultBatchSize = 1000

// ProcessOpt is a per-call option for ProcessMessages.
type ProcessOpt func(*processOpts)

type processOpts struct {
	batchSize int32
}

func defaultProcessOpts() *processOpts {
	return &processOpts{batchSize: defaultBatchSize}
}

// WithBatchSize sets the maximum number of messages ProcessMessages will
// acquire and hand to the Flusher in a single call. Must be > 0. Values
// above math.MaxInt32 are ignored and the default (1000) is used instead.
func WithBatchSize(n int) ProcessOpt {
	return func(opts *processOpts) {
		if n <= 0 || n > math.MaxInt32 {
			return
		}
		opts.batchSize = int32(n)
	}
}

// maintenanceLeaseTimeout is the age at which a held lease is considered
// abandoned and eligible for takeover by another instance. Exposed as a var
// so tests can shorten it without a config option.
var maintenanceLeaseTimeout = newAtomicDuration(30 * time.Second)

// topicScanInterval controls how often the scanner goroutine polls the topics
// table for newly registered topics. Exposed as a var for tests.
var topicScanInterval = newAtomicDuration(30 * time.Second)

// maintenanceIdleInterval is how long the maintenance goroutine sleeps when
// there are no messages at all for the topic.
const maintenanceIdleInterval = 30 * time.Second

// maintenanceRetryInterval is how long the maintenance goroutine sleeps after
// an error or after losing a lease competition.
const maintenanceRetryInterval = 5 * time.Second

// maintenanceMinInterval floors every sleep between maintenance-loop
// iterations, so a topic whose oldest message keeps hovering near the TTL
// boundary cannot re-peek in a tight loop. Exposed as a var for tests.
var maintenanceMinInterval = newAtomicDuration(5 * time.Second)

// maintenanceActivitySlack widens the window during which a topic counts as
// active beyond its TTL. It must cover the insert trigger's bump granularity
// (30s) plus at least one scan interval, so the final cleanup pass reliably
// runs before a topic goes dormant. Exposed as a var for tests.
var maintenanceActivitySlack = newAtomicDuration(2 * time.Minute)

// maintenanceCatchupInterval is how often the scanner force-includes every
// topic that still holds messages regardless of activity, recovering topics
// whose final cleanup pass was missed or whose TTL was configured after
// their last insert. Also runs on the first scan after startup. Exposed as a
// var for tests.
var maintenanceCatchupInterval = newAtomicDuration(time.Hour)

// consumerSessionRetention is how long an expired consumer-session row is
// kept before the catch-up sweep garbage-collects it. Instance ids are
// per-NewOutbox, so every process restart strands one row; sweeping them
// keeps the table at O(live instances). Deletion is semantically the same as
// expiry, so the buffer is caution against clock skew, not correctness.
// Exposed as a var for tests.
var consumerSessionRetention = newAtomicDuration(time.Hour)

// exclusiveLeaseDuration is how far ahead each consumer-session heartbeat
// pushes the session's expiry, and the length of the grace period granted
// when a holder's AcquireTopic ctx is cancelled. An instance that stops
// heartbeating loses its exclusive leases once this elapses.
var exclusiveLeaseDuration = newAtomicDuration(30 * time.Second)

// exclusiveLeaseRenewInterval controls how often the consumer-session
// heartbeat goroutine started by NewOutbox renews this instance's session.
var exclusiveLeaseRenewInterval = newAtomicDuration(10 * time.Second)

// exclusiveLeaseRetryInterval is how long AcquireTopic sleeps between attempts
// to acquire the lease when another instance currently holds it.
var exclusiveLeaseRetryInterval = newAtomicDuration(1 * time.Second)

type outboxImplOpts struct {
	schema            string
	autoMigrate       bool
	expirations       map[string]time.Duration
	defaultExpiration time.Duration
	logger            zerolog.Logger
}

func defaultOpts() *outboxImplOpts {
	return &outboxImplOpts{
		schema:      "outbox",
		autoMigrate: true,
		expirations: make(map[string]time.Duration),
		logger:      zerolog.Nop(),
	}
}

type outboxImpl struct {
	queries    *sqlc.Queries
	pool       *pgxpool.Pool
	schema     string
	instanceID uuid.UUID

	// expirations holds the per-topic TTLs configured at construction time via
	// WithTopicExpiration. Written to the topics table on Start so that any
	// instance can discover the TTL without its own config. Immutable after NewOutbox.
	expirations map[string]time.Duration

	// defaultExpiration is used for topics that have no specific TTL configured
	// via WithTopicExpiration. Zero means no default (those topics are skipped).
	defaultExpiration time.Duration

	// logger receives error-level messages from the background maintenance goroutines.
	logger zerolog.Logger

	flushers sync.Map

	// exclusiveLeaseWatcher holds one goroutine per held topic that waits for
	// the AcquireTopic ctx to end and then writes a single grace-period
	// expiry, ending that topic's deferral to this instance's consumer
	// session. AcquireTopic starts it per topic; ReleaseTopic stops it before
	// clearing the lease.
	exclusiveLeaseWatcher *leaseWatcher

	// managed tracks the maintenance-loop goroutine for each topic whose
	// maintenance lease this instance currently holds. Written only by the
	// scanner goroutine; the mutex exists for test introspection.
	managedMu sync.Mutex
	managed   map[string]*managedTopic
}

type OutboxOpt func(*outboxImplOpts)

func WithSchema(searchPath string) OutboxOpt {
	return func(opts *outboxImplOpts) {
		opts.schema = searchPath
	}
}

// WithAutoMigrate controls whether NewOutbox runs the embedded migrations on
// construction. Defaults to true. Set to false when the caller wants to run
// migrations explicitly via Migrate (for example, in a separate startup
// phase or release pipeline).
func WithAutoMigrate(enabled bool) OutboxOpt {
	return func(opts *outboxImplOpts) {
		opts.autoMigrate = enabled
	}
}

// WithTopicExpiration registers a TTL for the named topic. On Start, the TTL is
// written to the topics table so that any outbox instance can discover it.
// Messages older than ttl are eligible for deletion by the background maintenance
// goroutine launched by Start. Per-topic TTLs take precedence over
// WithDefaultExpiration.
func WithTopicExpiration(topic string, ttl time.Duration) OutboxOpt {
	return func(opts *outboxImplOpts) {
		opts.expirations[topic] = ttl
	}
}

// WithDefaultExpiration sets a fallback TTL used for topics that have no
// specific expiration configured via WithTopicExpiration. Any topic that
// appears in the topics table with a NULL expiration_nanos will be maintained
// using this TTL when Start is running.
func WithDefaultExpiration(ttl time.Duration) OutboxOpt {
	return func(opts *outboxImplOpts) {
		opts.defaultExpiration = ttl
	}
}

// WithLogger attaches a zerolog logger that receives error-level messages from
// the background maintenance goroutines. Lease competition (another instance
// holding the lease) is not logged. If not set, maintenance errors are silent.
func WithLogger(l zerolog.Logger) OutboxOpt {
	return func(opts *outboxImplOpts) {
		opts.logger = l
	}
}

// NewOutbox creates an outbox backed by pool and starts the background
// maintenance goroutines. The goroutines run until ctx is cancelled; pass a
// context tied to your application lifetime (e.g. from signal.NotifyContext).
func NewOutbox(ctx context.Context, pool *pgxpool.Pool, fs ...OutboxOpt) (Outbox, error) {
	opts := defaultOpts()

	for _, f := range fs {
		f(opts)
	}

	if err := validateSchemaName(opts.schema); err != nil {
		return nil, err
	}

	queries := sqlc.New()

	if opts.autoMigrate {
		if err := runMigrations(context.Background(), pool, opts.schema); err != nil {
			return nil, fmt.Errorf("could not run migrations: %w", err)
		}
	}

	expirations := make(map[string]time.Duration, len(opts.expirations))
	for k, v := range opts.expirations {
		expirations[k] = v
	}

	o := &outboxImpl{
		queries:           queries,
		pool:              pool,
		schema:            opts.schema,
		instanceID:        uuid.New(),
		expirations:       expirations,
		defaultExpiration: opts.defaultExpiration,
		logger:            opts.logger,
		managed:           make(map[string]*managedTopic),
	}
	o.exclusiveLeaseWatcher = newLeaseWatcher(o.expireExclusiveLeaseAfterGrace)

	for topic, ttl := range o.expirations {
		if err := o.ensureTopicRow(ctx, topic, ttl); err != nil {
			o.logger.Error().Err(err).Str("topic", topic).Msg("maintenance: failed to publish topic expiration")
		}
	}

	// Lenient like ensureTopicRow: with WithAutoMigrate(false) the tables may
	// not exist yet. The renewer below retries every renewal interval, and
	// AcquireTopic refreshes the session transactionally itself, so a failure
	// here only delays maintenance fair-share accounting.
	if err := o.upsertConsumerSession(ctx); err != nil {
		o.logger.Error().Err(err).Msg("exclusive consumer: failed to register consumer session")
	}

	go o.runConsumerSessionRenewer(ctx)
	go o.runTopicScanner(ctx)

	return o, nil
}

func (o *outboxImpl) AddFlusher(topic string, flusher Flusher) {
	o.flushers.Store(topic, flusher)
}

func (o *outboxImpl) getFlusher(topic string) (Flusher, bool) {
	f, ok := o.flushers.Load(topic)

	if !ok {
		return nil, false
	}

	flusher, ok := f.(Flusher)

	if !ok {
		return nil, false
	}

	return flusher, true
}

func (o *outboxImpl) AddMessages(ctx context.Context, tx pgx.Tx, topic string, msgs []MessageOpts) error {
	if topic == "" {
		return fmt.Errorf("topic must not be empty")
	}

	if len(msgs) == 0 {
		return nil
	}

	params := make([]sqlc.InsertMessageParams, len(msgs))

	for i, msg := range msgs {
		if len(msg.Payload) == 0 {
			return fmt.Errorf("payload for topic %q at index %d must not be empty", topic, i)
		}
		params[i] = sqlc.InsertMessageParams{
			Topic:   topic,
			Payload: msg.Payload,
		}
	}

	_, err := o.queries.InsertMessage(ctx, dbwrap.New(tx, o.schema), params)

	if err != nil {
		return fmt.Errorf("could not insert messages for topic %q: %w", topic, err)
	}

	return nil
}

func (o *outboxImpl) AcquireTopic(ctx context.Context, topic string) error {
	if topic == "" {
		return fmt.Errorf("topic must not be empty")
	}

	// A watcher left over from an earlier AcquireTopic for this topic must be
	// fully stopped before re-acquiring: its one action is writing an
	// expiring override, which would poison the fresh session-deferred lease
	// if it landed after the acquire below. Stop suppresses that write when
	// it wins the race, and when it loses (the old ctx ended first) it blocks
	// until the in-flight write has finished — either way the acquire always
	// has the last word. Stopping the watcher makes its expiry duty ours: if
	// we fail before installing a replacement, the old lease would stay
	// session-deferred forever with nothing left to expire it (the heartbeat
	// keeps the session fresh for the life of the process), so every failure
	// return below must write the expiry the watcher no longer will.
	stopped := o.exclusiveLeaseWatcher.Stop(topic)

	for {
		acquired, err := o.tryAcquireTopicLease(ctx, topic)
		if err != nil {
			o.expireLeaseIfWatcherOrphaned(ctx, topic, stopped)
			return err
		}
		if acquired {
			o.exclusiveLeaseWatcher.Start(ctx, topic)
			return nil
		}
		select {
		case <-ctx.Done():
			o.expireLeaseIfWatcherOrphaned(ctx, topic, stopped)
			return ctx.Err()
		case <-time.After(exclusiveLeaseRetryInterval.Load()):
		}
	}
}

// expireLeaseIfWatcherOrphaned writes the grace-period expiry for a lease
// whose watcher was stopped but never replaced — the failure paths of
// AcquireTopic and ReleaseTopic. It restores the pre-session behavior of "the
// last timestamp lapses naturally" for a hold the caller just walked away
// from. Detached ctx because the caller's ctx being cancelled is the most
// common way to land here; if even the detached write fails, the instance's
// heartbeat is failing against the same database, so the session itself
// lapses and takes the lease with it.
func (o *outboxImpl) expireLeaseIfWatcherOrphaned(ctx context.Context, topic string, hadWatcher bool) {
	if !hadWatcher {
		return
	}
	o.expireExclusiveLeaseAfterGrace(context.WithoutCancel(ctx), topic)
}

// ReleaseTopic stops this instance's lease renewer for topic and expires the
// lease immediately in the database, so another instance can acquire it right
// away instead of waiting out the lease duration. Like a naturally expired
// lease, the topic still requires an explicit AcquireTopic call afterward -
// released is not the same as never having been exclusive.
func (o *outboxImpl) ReleaseTopic(ctx context.Context, topic string) error {
	if topic == "" {
		return fmt.Errorf("topic must not be empty")
	}

	stopped := o.exclusiveLeaseWatcher.Stop(topic)

	expiredAt := time.Now().UTC().Add(-time.Second)
	err := o.queries.RenewTopicExclusiveConsumer(ctx, dbwrap.New(o.pool, o.schema), sqlc.RenewTopicExclusiveConsumerParams{
		Topic:                      topic,
		ExclusiveConsumerID:        &o.instanceID,
		ExclusiveConsumerExpiresAt: pgtype.Timestamptz{Time: expiredAt, Valid: true},
	})
	if err != nil {
		// The stopped watcher will never fire and the release write didn't
		// land (typically: caller ctx already cancelled) — without this the
		// lease would ride the session heartbeat forever. Degrades a failed
		// immediate release into a natural grace-period lapse.
		o.expireLeaseIfWatcherOrphaned(ctx, topic, stopped)
		return fmt.Errorf("release exclusive lease for topic %q: %w", topic, err)
	}
	return nil
}

// tryAcquireTopicLease makes one attempt to claim the exclusive lease. Returns
// (true, nil) on success, (false, nil) if another instance currently holds a
// valid lease, or (false, err) on a database error.
func (o *outboxImpl) tryAcquireTopicLease(ctx context.Context, topic string) (bool, error) {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin transaction for AcquireTopic %q: %w", topic, err)
	}
	defer tx.Rollback(ctx)

	wrapped := dbwrap.New(tx, o.schema)

	// Ensure a row exists; the trigger creates it on first message insert, but
	// AcquireTopic may be called before any messages exist for this topic.
	if err := o.queries.InsertTopicIfAbsent(ctx, wrapped, sqlc.InsertTopicIfAbsentParams{
		Topic:           topic,
		ExpirationNanos: pgtype.Int8{Valid: false},
	}); err != nil {
		return false, fmt.Errorf("ensure topic row for %q: %w", topic, err)
	}

	row, err := o.queries.GetTopicForUpdate(ctx, wrapped, topic)
	if err != nil {
		return false, fmt.Errorf("lock topic row for %q: %w", topic, err)
	}

	holderIsUs := row.ExclusiveConsumerID != nil && *row.ExclusiveConsumerID == o.instanceID
	leaseValid := row.ExclusiveConsumerExpiresAt.Valid && row.ExclusiveConsumerExpiresAt.Time.After(time.Now())

	if row.ExclusiveConsumerID != nil && !holderIsUs && leaseValid {
		return false, nil
	}

	// Refresh our consumer session in the same transaction: the lease written
	// below is only as alive as that session, and the background heartbeat may
	// not have landed yet (NewOutbox's initial upsert is best-effort — tables
	// missing under WithAutoMigrate(false), transient DB error). Failing the
	// acquire here beats returning success for a lease that immediately reads
	// as expired.
	if err := o.upsertConsumerSessionIn(ctx, wrapped); err != nil {
		return false, fmt.Errorf("refresh consumer session while acquiring topic %q: %w", topic, err)
	}

	// The per-topic timestamp is left NULL, so the lease defers to this
	// instance's consumer session for liveness (and any stale override from a
	// previous holder is cleared).
	if err := o.queries.SetTopicExclusiveConsumer(ctx, wrapped, sqlc.SetTopicExclusiveConsumerParams{
		Topic:                      topic,
		ExclusiveConsumerID:        &o.instanceID,
		ExclusiveConsumerExpiresAt: pgtype.Timestamptz{Valid: false},
	}); err != nil {
		return false, fmt.Errorf("set exclusive consumer for topic %q: %w", topic, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit exclusive lease for topic %q: %w", topic, err)
	}
	return true, nil
}

// expireExclusiveLeaseAfterGrace writes the one-shot per-topic override that
// ends a hold: the lease stops deferring to the consumer session and instead
// lapses one lease duration from now. Called by o.exclusiveLeaseWatcher with
// an already-detached ctx once the AcquireTopic ctx has ended. The WHERE
// clause makes this a silent no-op if another instance has since taken over.
func (o *outboxImpl) expireExclusiveLeaseAfterGrace(ctx context.Context, topic string) {
	expiresAt := time.Now().UTC().Add(exclusiveLeaseDuration.Load())
	err := o.queries.RenewTopicExclusiveConsumer(ctx, dbwrap.New(o.pool, o.schema), sqlc.RenewTopicExclusiveConsumerParams{
		Topic:                      topic,
		ExclusiveConsumerID:        &o.instanceID,
		ExclusiveConsumerExpiresAt: pgtype.Timestamptz{Time: expiresAt, Valid: true},
	})
	if err != nil {
		o.logger.Error().Err(err).Str("topic", topic).Msg("exclusive consumer: failed to write lease grace-period expiry")
	}
}

// upsertConsumerSession registers or refreshes this instance's row in
// consumer_sessions, the single per-instance heartbeat that exclusive and
// maintenance leases defer to for liveness.
func (o *outboxImpl) upsertConsumerSession(ctx context.Context) error {
	return o.upsertConsumerSessionIn(ctx, dbwrap.New(o.pool, o.schema))
}

func (o *outboxImpl) upsertConsumerSessionIn(ctx context.Context, wrapped *dbwrap.Wrapper) error {
	expiresAt := time.Now().UTC().Add(exclusiveLeaseDuration.Load())
	return o.queries.UpsertConsumerSession(ctx, wrapped, sqlc.UpsertConsumerSessionParams{
		ConsumerID:                 o.instanceID,
		ExpiresAt:                  pgtype.Timestamptz{Time: expiresAt, Valid: true},
		MaintainsDefaultExpiration: o.defaultExpiration > 0,
	})
}

// runConsumerSessionRenewer keeps this instance's consumer session fresh for
// the life of the outbox — one UPDATE per renewal interval regardless of how
// many topics are held.
func (o *outboxImpl) runConsumerSessionRenewer(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(exclusiveLeaseRenewInterval.Load()):
		}
		if err := o.upsertConsumerSession(ctx); err != nil && !errors.Is(err, context.Canceled) {
			o.logger.Error().Err(err).Msg("exclusive consumer: failed to renew consumer session")
		}
	}
}

// checkExclusiveAccessForUpdate locks the topic row (FOR UPDATE OF t — the
// joined consumer_sessions row is deliberately left unlocked, since the
// holder's every other topic reads it too) within the caller's transaction
// and returns an error if another instance holds an active exclusive lease.
// A missing topics row (topic not yet seen) is treated as non-exclusive.
//
// The row lock is held until the caller's transaction commits or rolls back,
// so a concurrent AcquireTopic or lease renewal for this topic blocks until
// then instead of being able to hand the lease to another instance while this
// transaction is still relying on the check having passed. That closes the
// gap between validating ownership and committing the work done under it.
func (o *outboxImpl) checkExclusiveAccessForUpdate(ctx context.Context, wrapped *dbwrap.Wrapper, topic string) error {
	row, err := o.queries.GetTopicForUpdate(ctx, wrapped, topic)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		return fmt.Errorf("check exclusive access for topic %q: %w", topic, err)
	}

	if row.ExclusiveConsumerID == nil {
		return nil
	}

	if *row.ExclusiveConsumerID == o.instanceID {
		if row.ExclusiveConsumerExpiresAt.Valid && row.ExclusiveConsumerExpiresAt.Time.After(time.Now()) {
			return nil
		}
		return fmt.Errorf("exclusive lease for topic %q has expired; call AcquireTopic to renew", topic)
	}

	if row.ExclusiveConsumerExpiresAt.Valid && row.ExclusiveConsumerExpiresAt.Time.After(time.Now()) {
		return ErrExclusiveLeaseHeld
	}

	// Another instance held the lease but it has expired; require explicit acquire.
	return fmt.Errorf("exclusive access required for topic %q: call AcquireTopic first", topic)
}

func (o *outboxImpl) ProcessMessages(ctx context.Context, topic string, popts ...ProcessOpt) ([]*sqlc.Message, error) {
	f, ok := o.getFlusher(topic)

	if !ok {
		return nil, fmt.Errorf("no flusher registered for topic %q", topic)
	}

	po := defaultProcessOpts()
	for _, opt := range popts {
		opt(po)
	}

	tx, err := o.pool.Begin(ctx)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction for processing messages for topic %q: %w", topic, err)
	}

	defer tx.Rollback(ctx)

	wrapped := dbwrap.New(tx, o.schema)

	if err := o.checkExclusiveAccessForUpdate(ctx, wrapped, topic); err != nil {
		return nil, err
	}

	msgs, err := o.queries.AcquireMessagesByTopic(ctx, wrapped, sqlc.AcquireMessagesByTopicParams{
		Topic: topic,
		Limit: pgtype.Int4{
			Int32: po.batchSize,
			Valid: true,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("could not acquire messages for topic %q: %w", topic, err)
	}

	if len(msgs) == 0 {
		// just commit to avoid rollback monitoring
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("could not commit transaction for processing messages for topic %q: %w", topic, err)
		}

		return nil, nil
	}

	err = f.Flush(&flushContext{Context: ctx, tx: tx}, msgs)

	if err != nil {
		return nil, fmt.Errorf("flusher failed for topic %q: %w", topic, err)
	}

	// delete the messages that were flushed
	msgIDs := make([]int64, len(msgs))

	for i, msg := range msgs {
		msgIDs[i] = msg.ID
	}

	err = o.queries.DeleteMessagesByIds(ctx, wrapped, sqlc.DeleteMessagesByIdsParams{
		Topic: topic,
		Ids:   msgIDs,
	})

	if err != nil {
		return nil, fmt.Errorf("could not delete messages for topic %q: %w", topic, err)
	}

	// This transaction holds the topic row lock (checkExclusiveAccessForUpdate)
	// for its whole lifetime, which makes the insert trigger's SKIP LOCKED
	// activity bump skip this topic — on a continuously-processed topic the
	// trigger could be starved out of every bump and the topic would read as
	// dormant despite live traffic. Since we just processed messages and
	// already hold the lock that causes the starvation, refresh the activity
	// stamp here; the query self-gates to the trigger's 30s granularity, so
	// this is at most one extra row write per topic per window.
	if err := o.queries.BumpTopicActivityIfStale(ctx, wrapped, topic); err != nil {
		return nil, fmt.Errorf("could not bump activity for topic %q: %w", topic, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("could not commit transaction for processing messages for topic %q: %w", topic, err)
	}

	return msgs, nil
}

// managedTopic tracks one running maintenance loop. dormant distinguishes a
// scanner-initiated stop (topic left the active window: final cleanup pass
// and lease release on the way out) from plain shutdown (exit with zero
// writes; the consumer session lapsing is the release).
type managedTopic struct {
	cancel  context.CancelFunc
	done    chan struct{}
	dormant atomic.Bool
}

// runTopicScanner periodically scans for active topics (last insert within
// TTL + slack), claims a fair share of their maintenance leases for this
// instance, and manages one maintenance goroutine per claimed topic. Claimed
// leases carry no expiry of their own: they stay valid exactly as long as
// this instance's consumer session, so holding them costs zero per-topic
// writes and a crashed instance's topics free up as soon as its session
// lapses. Dormant topics cost nothing on any instance: no goroutine, no
// peek queries, no lease row.
func (o *outboxImpl) runTopicScanner(ctx context.Context) {
	var lastCatchup time.Time

	for {
		if ctx.Err() != nil {
			return
		}

		lastCatchup = o.scanTopics(ctx, lastCatchup)

		select {
		case <-ctx.Done():
			return
		case <-time.After(topicScanInterval.Load()):
		}
	}
}

// scanTopics runs one scanner tick and returns the updated time of the last
// successful catch-up sweep.
func (o *outboxImpl) scanTopics(ctx context.Context, lastCatchup time.Time) time.Time {
	wrapped := dbwrap.New(o.pool, o.schema)

	o.reapExitedLoops()

	// The catch-up sweep force-includes every topic that still holds
	// messages, active or not: it recovers topics whose final cleanup pass
	// was missed (e.g. the whole fleet was down at the wrong moment) and
	// topics whose TTL was configured after their last insert was already
	// older than it. The zero time on the first tick makes it always run at
	// startup.
	include := []string{}
	if time.Since(lastCatchup) >= maintenanceCatchupInterval.Load() {
		topics, err := o.queries.GetTopicsWithMessages(ctx, wrapped)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				o.logger.Error().Err(err).Msg("maintenance: failed to list topics with messages for catch-up sweep")
			}
		} else {
			include = topics
			lastCatchup = time.Now()
		}

		// Best-effort GC of session rows stranded by dead instances, on the
		// same cadence. Every instance runs it; the guarded DELETE makes the
		// overlap harmless.
		cutoff := time.Now().UTC().Add(-consumerSessionRetention.Load())
		if err := o.queries.DeleteExpiredConsumerSessions(ctx, wrapped,
			pgtype.Timestamptz{Time: cutoff, Valid: true}); err != nil && !errors.Is(err, context.Canceled) {
			o.logger.Error().Err(err).Msg("maintenance: failed to delete expired consumer sessions")
		}
	}

	defaultNanos := pgtype.Int8{}
	if o.defaultExpiration > 0 {
		defaultNanos = pgtype.Int8{Int64: o.defaultExpiration.Nanoseconds(), Valid: true}
	}

	candidates, err := o.queries.GetMaintenanceCandidateTopics(ctx, wrapped, sqlc.GetMaintenanceCandidateTopicsParams{
		HolderID:               o.instanceID,
		StaleCutoff:            pgtype.Timestamptz{Time: time.Now().UTC().Add(-maintenanceLeaseTimeout.Load()), Valid: true},
		DefaultExpirationNanos: defaultNanos,
		IncludeTopics:          include,
		SlackSeconds:           maintenanceActivitySlack.Load().Seconds(),
	})
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			o.logger.Error().Err(err).Msg("maintenance: failed to scan topics table")
		}
		return lastCatchup
	}

	ttls := make(map[string]time.Duration, len(candidates))
	var held []string
	var explicit, dflt claimClass
	for _, c := range candidates {
		ttl := o.defaultExpiration
		class := &dflt
		if c.ExpirationNanos.Valid {
			ttl = time.Duration(c.ExpirationNanos.Int64)
			class = &explicit
		}
		if ttl <= 0 {
			continue
		}
		ttls[c.Topic] = ttl
		class.candidates++
		switch {
		case c.HeldByMe:
			class.held++
			held = append(held, c.Topic)
		case c.Unleased:
			class.unleased = append(class.unleased, c.Topic)
		case c.LeaseExpired:
			class.stale = append(class.stale, c.Topic)
		}
	}

	held = append(held, o.claimTopics(ctx, wrapped, &explicit, &dflt)...)

	o.reconcileLoops(ctx, ttls, held)
	return lastCatchup
}

// claimClass groups one scanner tick's claim state for one fair-share class
// of topics. The classes exist because eligibility differs: topics with an
// explicit TTL (expiration_nanos in the DB) are claimable by every live
// instance, while default-TTL topics are only visible to instances
// configured with WithDefaultExpiration — dividing those by the total live
// count would leave most of a default-TTL backlog to the floor-of-1 trickle
// whenever producer-/process-only instances outnumber the maintainers.
type claimClass struct {
	candidates int      // candidate topics in this class this tick
	held       int      // of those, already held by this instance
	unleased   []string // claimable: no lease row yet
	stale      []string // claimable: lease row whose holder is gone
}

// claimTopics claims up to this instance's fair share of the claimable
// topics in each class, dividing explicit-TTL candidates by all live
// sessions and default-TTL candidates by the sessions flagged as default
// maintainers. Returns the topics actually won.
func (o *outboxImpl) claimTopics(ctx context.Context, wrapped *dbwrap.Wrapper, explicit, dflt *claimClass) []string {
	if len(explicit.unleased)+len(explicit.stale)+len(dflt.unleased)+len(dflt.stale) == 0 {
		return nil
	}

	counts, err := o.queries.CountLiveConsumerSessions(ctx, wrapped)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			o.logger.Error().Err(err).Msg("maintenance: failed to count live consumer sessions")
		}
		return nil
	}

	won := o.claimClassShare(ctx, wrapped, explicit, counts.Live)
	return append(won, o.claimClassShare(ctx, wrapped, dflt, counts.DefaultMaintainers)...)
}

// claimClassShare claims up to ceil(candidates/instances) - held of one
// class's claimable topics, in random order so concurrently scanning
// instances spread across the set instead of contending on the same prefix.
// The fair-share cap spreads load; it is not a correctness constraint. The
// floor of 1 keeps claimable topics from being stranded (e.g. this
// instance's own session row hasn't landed yet, so the class denominator
// under-counts). Claims lost to SKIP LOCKED or the insert conflict arm
// simply come back around on a later scan.
func (o *outboxImpl) claimClassShare(ctx context.Context, wrapped *dbwrap.Wrapper, class *claimClass, instances int64) []string {
	if len(class.unleased)+len(class.stale) == 0 {
		return nil
	}
	if instances < 1 {
		instances = 1
	}

	budget := max((class.candidates+int(instances)-1)/int(instances)-class.held, 1)

	rand.Shuffle(len(class.unleased), func(i, j int) { class.unleased[i], class.unleased[j] = class.unleased[j], class.unleased[i] })
	rand.Shuffle(len(class.stale), func(i, j int) { class.stale[i], class.stale[j] = class.stale[j], class.stale[i] })

	var won []string
	if len(class.unleased) > 0 {
		topics, err := o.queries.InsertMaintenanceLeases(ctx, wrapped, sqlc.InsertMaintenanceLeasesParams{
			Topics:   class.unleased[:min(budget, len(class.unleased))],
			HolderID: o.instanceID,
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				o.logger.Error().Err(err).Msg("maintenance: failed to claim unleased topics")
			}
		} else {
			won = append(won, topics...)
			budget -= len(topics)
		}
	}
	if budget > 0 && len(class.stale) > 0 {
		topics, err := o.queries.ClaimMaintenanceLeases(ctx, wrapped, sqlc.ClaimMaintenanceLeasesParams{
			Topics:      class.stale[:min(budget, len(class.stale))],
			HolderID:    o.instanceID,
			StaleCutoff: pgtype.Timestamptz{Time: time.Now().UTC().Add(-maintenanceLeaseTimeout.Load()), Valid: true},
		})
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				o.logger.Error().Err(err).Msg("maintenance: failed to take over expired maintenance leases")
			}
		} else {
			won = append(won, topics...)
		}
	}
	return won
}

// reconcileLoops starts a maintenance goroutine for every held topic that
// doesn't have one and cancels goroutines whose topic left the candidate set
// (dormancy: final cleanup pass and lease release happen on the way out) or
// whose lease this instance no longer holds (exit with no writes). A
// cancelled loop stays in managed until reaped, so a topic that reactivates
// during its dormancy exit can never end up with two loops.
func (o *outboxImpl) reconcileLoops(ctx context.Context, ttls map[string]time.Duration, held []string) {
	heldSet := make(map[string]struct{}, len(held))
	for _, topic := range held {
		heldSet[topic] = struct{}{}
	}

	o.managedMu.Lock()
	defer o.managedMu.Unlock()

	for topic, mt := range o.managed {
		if _, active := ttls[topic]; !active {
			mt.dormant.Store(true)
			mt.cancel()
		} else if _, ours := heldSet[topic]; !ours {
			mt.cancel()
		}
	}

	for _, topic := range held {
		if _, ok := o.managed[topic]; ok {
			continue
		}
		loopCtx, cancel := context.WithCancel(ctx)
		mt := &managedTopic{cancel: cancel, done: make(chan struct{})}
		o.managed[topic] = mt
		go o.runMaintenanceLoop(loopCtx, topic, ttls[topic], mt)
	}
}

// reapExitedLoops drops managed entries whose goroutine has fully exited.
func (o *outboxImpl) reapExitedLoops() {
	o.managedMu.Lock()
	defer o.managedMu.Unlock()
	for topic, mt := range o.managed {
		select {
		case <-mt.done:
			delete(o.managed, topic)
		default:
		}
	}
}

func (o *outboxImpl) runMaintenanceLoop(ctx context.Context, topic string, ttl time.Duration, mt *managedTopic) {
	defer close(mt.done)

	// sleep waits at least maintenanceMinInterval and reports false once ctx
	// has ended.
	sleep := func(d time.Duration) bool {
		if floor := maintenanceMinInterval.Load(); d < floor {
			d = floor
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d):
			return true
		}
	}

	for ctx.Err() == nil {
		wrapped := dbwrap.New(o.pool, o.schema)

		oldest, err := o.queries.GetOldestMessageInsertedAt(ctx, wrapped, topic)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if !sleep(maintenanceIdleInterval) {
					break
				}
				continue
			}
			if !errors.Is(err, context.Canceled) {
				o.logger.Error().Err(err).Str("topic", topic).Msg("maintenance: failed to peek oldest message")
			}
			if !sleep(maintenanceRetryInterval) {
				break
			}
			continue
		}

		if !oldest.Valid {
			if !sleep(maintenanceIdleInterval) {
				break
			}
			continue
		}

		expireAt := oldest.Time.Add(ttl)
		if now := time.Now(); expireAt.After(now) {
			if !sleep(expireAt.Sub(now)) {
				break
			}
			continue
		}

		if err := o.runMaintenance(ctx, topic, ttl); err != nil {
			if !errors.Is(err, errLeaseHeld) && !errors.Is(err, context.Canceled) {
				o.logger.Error().Err(err).Str("topic", topic).Msg("maintenance: failed to run cleanup")
			}
		}
		if !sleep(maintenanceRetryInterval) {
			break
		}
	}

	if mt.dormant.Load() {
		o.finishDormantTopic(context.WithoutCancel(ctx), topic, ttl)
	}
}

// finishDormantTopic is the dormancy exit for a maintenance loop: by the time
// the scanner stops a topic, every message it still holds is older than its
// TTL by construction (the activity window is TTL + slack), so one final
// cleanup pass empties it. The lease row is deleted only after an empty-topic
// check: if a message raced in, the insert trigger has already re-marked the
// topic active and a later scan re-claims it, so leaving the lease in place
// loses nothing.
func (o *outboxImpl) finishDormantTopic(ctx context.Context, topic string, ttl time.Duration) {
	if err := o.runMaintenance(ctx, topic, ttl); err != nil {
		if !errors.Is(err, errLeaseHeld) {
			o.logger.Error().Err(err).Str("topic", topic).Msg("maintenance: failed to run final cleanup for dormant topic")
		}
		return
	}

	wrapped := dbwrap.New(o.pool, o.schema)
	_, err := o.queries.GetOldestMessageInsertedAt(ctx, wrapped, topic)
	if err == nil {
		return
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		o.logger.Error().Err(err).Str("topic", topic).Msg("maintenance: failed to verify dormant topic is empty")
		return
	}

	if err := o.queries.ReleaseMaintenanceLease(ctx, wrapped, sqlc.ReleaseMaintenanceLeaseParams{
		Topic:    topic,
		HolderID: o.instanceID,
	}); err != nil {
		o.logger.Error().Err(err).Str("topic", topic).Msg("maintenance: failed to release lease for dormant topic")
	}
}

var errLeaseHeld = errors.New("maintenance lease held by another instance")

func (o *outboxImpl) runMaintenance(ctx context.Context, topic string, ttl time.Duration) error {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin maintenance tx for topic %q: %w", topic, err)
	}
	defer tx.Rollback(ctx)

	wrapped := dbwrap.New(tx, o.schema)

	if err := o.queries.InsertMaintenanceLeaseIfAbsent(ctx, wrapped, sqlc.InsertMaintenanceLeaseIfAbsentParams{
		Topic:    topic,
		HolderID: o.instanceID,
	}); err != nil {
		return fmt.Errorf("upsert maintenance lease for topic %q: %w", topic, err)
	}

	lease, err := o.queries.SelectMaintenanceLeaseForUpdate(ctx, wrapped, sqlc.SelectMaintenanceLeaseForUpdateParams{
		Topic:       topic,
		StaleCutoff: pgtype.Timestamptz{Time: time.Now().UTC().Add(-maintenanceLeaseTimeout.Load()), Valid: true},
	})
	if err != nil {
		return fmt.Errorf("select maintenance lease for topic %q: %w", topic, err)
	}

	if lease.HolderID != o.instanceID && !lease.LeaseExpired {
		return errLeaseHeld
	}

	if err := o.queries.UpdateMaintenanceLease(ctx, wrapped, sqlc.UpdateMaintenanceLeaseParams{
		Topic:    topic,
		HolderID: o.instanceID,
	}); err != nil {
		return fmt.Errorf("update maintenance lease for topic %q: %w", topic, err)
	}

	cutoff := time.Now().UTC().Add(-ttl)
	if err := o.queries.DeleteExpiredMessages(ctx, wrapped, sqlc.DeleteExpiredMessagesParams{
		Topic: topic,
		Cutoff: pgtype.Timestamptz{
			Time:  cutoff,
			Valid: true,
		},
	}); err != nil {
		return fmt.Errorf("delete expired messages for topic %q: %w", topic, err)
	}

	return tx.Commit(ctx)
}

// ensureTopicRow writes the topic and its TTL to the topics table. Uses o.pool
// directly so the row is committed independently of any caller transaction.
func (o *outboxImpl) ensureTopicRow(ctx context.Context, topic string, ttl time.Duration) error {
	expNanos := pgtype.Int8{Valid: false}
	if ttl > 0 {
		expNanos = pgtype.Int8{Int64: ttl.Nanoseconds(), Valid: true}
	}
	return o.queries.InsertTopicIfAbsent(ctx, dbwrap.New(o.pool, o.schema), sqlc.InsertTopicIfAbsentParams{
		Topic:           topic,
		ExpirationNanos: expNanos,
	})
}
