package pgoutbox

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hatchet-dev/pgoutbox/internal/dbwrap"
	"github.com/hatchet-dev/pgoutbox/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

type Flusher interface {
	Flush(ctx context.Context, msgs []*sqlc.Message) error
}

// TxFlusher is an optional extension of Flusher. When a registered flusher
// implements it, ProcessMessages calls FlushWithTx instead of Flush, handing
// over the same pgx.Tx it used to acquire the messages. This lets the flusher
// run its own writes in the same transaction that deletes the flushed
// messages, so the flush and the delete commit (or roll back) atomically.
type TxFlusher interface {
	Flusher

	FlushWithTx(ctx context.Context, tx pgx.Tx, msgs []*sqlc.Message) error
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
	ProcessMessages(ctx context.Context, topic string) ([]*sqlc.Message, error)

	// AcquireTopic blocks until this instance holds the exclusive processing lease
	// for the named topic, then returns. A background goroutine automatically renews
	// the lease until ctx is cancelled, at which point the lease expires naturally
	// and another instance can take over. AcquireTopic must be called before
	// ProcessMessages for any topic that has an active exclusive consumer.
	AcquireTopic(ctx context.Context, topic string) error

	// Start launches a background scanner that discovers topics with configured
	// expirations from the topics table and runs per-topic maintenance goroutines.
	// Goroutines terminate when ctx is cancelled. Start is a no-op when no topics
	// with expirations exist in the database.
	Start(ctx context.Context)
}

// ErrExclusiveLeaseHeld is returned by AcquireTopic when another outbox instance
// currently holds a valid exclusive lease for the topic.
var ErrExclusiveLeaseHeld = errors.New("exclusive lease held by another instance")

// defaultBatchSize is the number of messages ProcessMessages will pull per
// call when the caller has not overridden it via WithBatchSize.
const defaultBatchSize = 100

// maintenanceLeaseTimeout is the age at which a held lease is considered
// abandoned and eligible for takeover by another instance. Exposed as a var
// so tests can shorten it without a config option.
var maintenanceLeaseTimeout = 30 * time.Second

// topicScanInterval controls how often the scanner goroutine polls the topics
// table for newly registered topics. Exposed as a var for tests.
var topicScanInterval = 30 * time.Second

// maintenanceIdleInterval is how long the maintenance goroutine sleeps when
// there are no messages at all for the topic.
const maintenanceIdleInterval = 30 * time.Second

// maintenanceRetryInterval is how long the maintenance goroutine sleeps after
// an error or after losing a lease competition.
const maintenanceRetryInterval = 5 * time.Second

// exclusiveLeaseDuration is how long each exclusive consumer lease grant lasts.
// The renewer refreshes the lease before this elapses.
var exclusiveLeaseDuration = 30 * time.Second

// exclusiveLeaseRenewInterval controls how often the background goroutine
// started by AcquireTopic renews the exclusive lease.
var exclusiveLeaseRenewInterval = 10 * time.Second

// exclusiveLeaseRetryInterval is how long AcquireTopic sleeps between attempts
// to acquire the lease when another instance currently holds it.
var exclusiveLeaseRetryInterval = 1 * time.Second

type outboxImplOpts struct {
	schema            string
	batchSize         int32
	autoMigrate       bool
	expirations       map[string]time.Duration
	defaultExpiration time.Duration
	logger            zerolog.Logger
}

func defaultOpts() *outboxImplOpts {
	return &outboxImplOpts{
		schema:      "outbox",
		batchSize:   defaultBatchSize,
		autoMigrate: true,
		expirations: make(map[string]time.Duration),
		logger:      zerolog.Nop(),
	}
}

type outboxImpl struct {
	queries    *sqlc.Queries
	pool       *pgxpool.Pool
	schema     string
	batchSize  int32
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
}

type OutboxOpt func(*outboxImplOpts)

func WithSchema(searchPath string) OutboxOpt {
	return func(opts *outboxImplOpts) {
		opts.schema = searchPath
	}
}

// WithBatchSize sets the maximum number of messages ProcessMessages will
// acquire and hand to the Flusher per call. Must be > 0. Values are clamped
// to int32; sizes above math.MaxInt32 fall back to the default.
func WithBatchSize(n int) OutboxOpt {
	return func(opts *outboxImplOpts) {
		if n <= 0 || n > math.MaxInt32 {
			return
		}
		opts.batchSize = int32(n)
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

// WithTopicExpiration registers a TTL for the named topic. On the first
// AddMessages call for that topic (and on Start), the TTL is written to the
// topics table so that any outbox instance can discover it. Messages older
// than ttl are eligible for deletion by the background maintenance goroutine
// launched by Start. Per-topic TTLs take precedence over WithDefaultExpiration.
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

func NewOutbox(pool *pgxpool.Pool, fs ...OutboxOpt) (Outbox, error) {
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

	return &outboxImpl{
		queries:           queries,
		pool:              pool,
		schema:            opts.schema,
		batchSize:         opts.batchSize,
		instanceID:        uuid.New(),
		expirations:       expirations,
		defaultExpiration: opts.defaultExpiration,
		logger:            opts.logger,
	}, nil
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
	for {
		acquired, err := o.tryAcquireTopicLease(ctx, topic)
		if err != nil {
			return err
		}
		if acquired {
			go o.runExclusiveLeaseRenewer(ctx, topic)
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(exclusiveLeaseRetryInterval):
		}
	}
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

	expiresAt := time.Now().UTC().Add(exclusiveLeaseDuration)
	if err := o.queries.SetTopicExclusiveConsumer(ctx, wrapped, sqlc.SetTopicExclusiveConsumerParams{
		Topic:                      topic,
		ExclusiveConsumerID:        &o.instanceID,
		ExclusiveConsumerExpiresAt: pgtype.Timestamptz{Time: expiresAt, Valid: true},
	}); err != nil {
		return false, fmt.Errorf("set exclusive consumer for topic %q: %w", topic, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit exclusive lease for topic %q: %w", topic, err)
	}
	return true, nil
}

// runExclusiveLeaseRenewer periodically extends the exclusive consumer lease for
// topic until ctx is cancelled. When ctx is done the lease expires naturally,
// allowing another instance to acquire the topic.
func (o *outboxImpl) runExclusiveLeaseRenewer(ctx context.Context, topic string) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(exclusiveLeaseRenewInterval):
		}

		expiresAt := time.Now().UTC().Add(exclusiveLeaseDuration)
		err := o.queries.RenewTopicExclusiveConsumer(ctx, dbwrap.New(o.pool, o.schema), sqlc.RenewTopicExclusiveConsumerParams{
			Topic:                      topic,
			ExclusiveConsumerID:        &o.instanceID,
			ExclusiveConsumerExpiresAt: pgtype.Timestamptz{Time: expiresAt, Valid: true},
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			o.logger.Error().Err(err).Str("topic", topic).Msg("exclusive consumer: failed to renew lease")
		}
	}
}

// checkExclusiveAccess returns an error if the topic has an active exclusive
// consumer that is not this instance. A missing topics row (topic not yet seen)
// is treated as non-exclusive.
func (o *outboxImpl) checkExclusiveAccess(ctx context.Context, topic string) error {
	row, err := o.queries.GetTopicExclusiveStatus(ctx, dbwrap.New(o.pool, o.schema), topic)
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

func (o *outboxImpl) ProcessMessages(ctx context.Context, topic string) ([]*sqlc.Message, error) {
	f, ok := o.getFlusher(topic)

	if !ok {
		return nil, fmt.Errorf("no flusher registered for topic %q", topic)
	}

	if err := o.checkExclusiveAccess(ctx, topic); err != nil {
		return nil, err
	}

	tx, err := o.pool.Begin(ctx)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction for processing messages for topic %q: %w", topic, err)
	}

	defer tx.Rollback(ctx)

	wrapped := dbwrap.New(tx, o.schema)

	msgs, err := o.queries.AcquireMessagesByTopic(ctx, wrapped, sqlc.AcquireMessagesByTopicParams{
		Topic: topic,
		Limit: pgtype.Int4{
			Int32: o.batchSize,
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

	// call the flusher. If it can operate within our transaction, hand it the
	// tx so its writes commit atomically with the delete below.
	if tf, ok := f.(TxFlusher); ok {
		err = tf.FlushWithTx(ctx, tx, msgs)
	} else {
		err = f.Flush(ctx, msgs)
	}

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

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("could not commit transaction for processing messages for topic %q: %w", topic, err)
	}

	return msgs, nil
}

func (o *outboxImpl) Start(ctx context.Context) {
	// Publish TTLs for all locally-configured topics so that any instance
	// (including maintenance-only ones that never call AddMessages) can
	// discover them from the topics table.
	for topic, ttl := range o.expirations {
		_ = o.ensureTopicRow(ctx, topic, ttl)
	}

	go o.runTopicScanner(ctx)
}

// runTopicScanner polls the topics table and spawns a maintenance goroutine
// for each topic that is not already being managed. TTL resolution order:
// per-topic expiration_nanos from the DB, then defaultExpiration, then skip.
func (o *outboxImpl) runTopicScanner(ctx context.Context) {
	managed := make(map[string]struct{})

	for {
		if ctx.Err() != nil {
			return
		}

		rows, err := o.queries.GetAllTopics(ctx, dbwrap.New(o.pool, o.schema))
		if err != nil {
			o.logger.Error().Err(err).Msg("maintenance: failed to scan topics table")
		} else {
			for _, row := range rows {
				if _, ok := managed[row.Topic]; ok {
					continue
				}

				var ttl time.Duration
				if row.ExpirationNanos.Valid {
					ttl = time.Duration(row.ExpirationNanos.Int64)
				} else {
					ttl = o.defaultExpiration
				}

				if ttl == 0 {
					continue
				}

				managed[row.Topic] = struct{}{}
				go o.runMaintenanceLoop(ctx, row.Topic, ttl)
			}
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(topicScanInterval):
		}
	}
}

func (o *outboxImpl) runMaintenanceLoop(ctx context.Context, topic string, ttl time.Duration) {
	for {
		if ctx.Err() != nil {
			return
		}

		wrapped := dbwrap.New(o.pool, o.schema)

		oldest, err := o.queries.GetOldestMessageInsertedAt(ctx, wrapped, topic)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(maintenanceIdleInterval):
				}
				continue
			}
			o.logger.Error().Err(err).Str("topic", topic).Msg("maintenance: failed to peek oldest message")
			select {
			case <-ctx.Done():
				return
			case <-time.After(maintenanceRetryInterval):
			}
			continue
		}

		if !oldest.Valid {
			select {
			case <-ctx.Done():
				return
			case <-time.After(maintenanceIdleInterval):
			}
			continue
		}

		expireAt := oldest.Time.Add(ttl)
		if now := time.Now(); expireAt.After(now) {
			select {
			case <-ctx.Done():
				return
			case <-time.After(expireAt.Sub(now)):
			}
			continue
		}

		if err := o.runMaintenance(ctx, topic, ttl); err != nil {
			if !errors.Is(err, errLeaseHeld) {
				o.logger.Error().Err(err).Str("topic", topic).Msg("maintenance: failed to run cleanup")
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(maintenanceRetryInterval):
			}
			continue
		}
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

	lease, err := o.queries.SelectMaintenanceLeaseForUpdate(ctx, wrapped, topic)
	if err != nil {
		return fmt.Errorf("select maintenance lease for topic %q: %w", topic, err)
	}

	holdsLease := lease.HolderID == o.instanceID
	leaseExpired := time.Since(lease.AcquiredAt.Time) > maintenanceLeaseTimeout

	if !holdsLease && !leaseExpired {
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
