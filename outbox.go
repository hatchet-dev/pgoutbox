package pgoutbox

import (
	"context"
	"fmt"
	"math"
	"sync"

	"github.com/hatchet-dev/pgoutbox/internal/dbwrap"
	"github.com/hatchet-dev/pgoutbox/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Flusher interface {
	Flush(ctx context.Context, msgs []*sqlc.Message) error
}

type MessageOpts struct {
	Payload []byte
}

type Outbox interface {
	AddFlusher(topic string, flusher Flusher)

	AddMessages(ctx context.Context, tx pgx.Tx, topic string, msgs []MessageOpts) error

	// ProcessMessages grabs a batch of messages for the given topic, flushes them using the registered Flusher for that
	// topic, and deletes them from the outbox if the flush is successful.
	ProcessMessages(ctx context.Context, topic string) ([]*sqlc.Message, error)
}

// defaultBatchSize is the number of messages ProcessMessages will pull per
// call when the caller has not overridden it via WithBatchSize.
const defaultBatchSize = 100

type outboxImplOpts struct {
	schema      string
	batchSize   int32
	autoMigrate bool
}

func defaultOpts() *outboxImplOpts {
	return &outboxImplOpts{
		schema:      "outbox",
		batchSize:   defaultBatchSize,
		autoMigrate: true,
	}
}

type outboxImpl struct {
	queries   *sqlc.Queries
	pool      *pgxpool.Pool
	schema    string
	batchSize int32

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

	return &outboxImpl{
		queries:   queries,
		pool:      pool,
		schema:    opts.schema,
		batchSize: opts.batchSize,
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

func (o *outboxImpl) ProcessMessages(ctx context.Context, topic string) ([]*sqlc.Message, error) {
	f, ok := o.getFlusher(topic)

	if !ok {
		return nil, fmt.Errorf("no flusher registered for topic %q", topic)
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

	// call the flusher
	err = f.Flush(ctx, msgs)

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
