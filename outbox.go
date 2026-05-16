package pgoutbox

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/hatchet-dev/pgoutbox/internal/dbwrap"
	"github.com/hatchet-dev/pgoutbox/internal/partitions"
	"github.com/hatchet-dev/pgoutbox/internal/sizing"
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

	// ProcessMessages grabs a batch of messages for the given topic, flushes
	// them using the registered Flusher, and advances the per-topic ack cursor
	// if the flush succeeds. Message rows are append-only and are removed only
	// when a fully-acked sealed partition is dropped.
	ProcessMessages(ctx context.Context, topic string) ([]*sqlc.Message, error)
}

// defaultBatchSize is the number of messages ProcessMessages will pull per
// call when the caller has not overridden it via WithBatchSize.
const defaultBatchSize = 100
const defaultPartitionSize int64 = 100_000
const defaultPartitionCount = 2

type outboxImplOpts struct {
	schema         string
	batchSize      int32
	autoMigrate    bool
	partitionSize  int64
	partitionCount int
}

func defaultOpts() *outboxImplOpts {
	return &outboxImplOpts{
		schema:         "outbox",
		batchSize:      defaultBatchSize,
		autoMigrate:    true,
		partitionSize:  defaultPartitionSize,
		partitionCount: defaultPartitionCount,
	}
}

type outboxImpl struct {
	queries          *sqlc.Queries
	pool             *pgxpool.Pool
	schema           string
	batchSize        int32
	partitionSize    int64
	partitionCount   int
	partitionManager *partitions.Manager

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

// WithDefaultPartitionSize sets the default target number of rows per
// partition segment for newly-created topics.
func WithDefaultPartitionSize(z int64) OutboxOpt {
	return func(opts *outboxImplOpts) {
		if z <= 0 {
			return
		}
		opts.partitionSize = z
	}
}

// WithDefaultPartitionCount sets the number of future partitions maintained
// ahead of the current write position for newly-created topics.
func WithDefaultPartitionCount(n int) OutboxOpt {
	return func(opts *outboxImplOpts) {
		if n <= 0 || n > math.MaxInt32 {
			return
		}
		opts.partitionCount = n
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
		queries:          queries,
		pool:             pool,
		schema:           opts.schema,
		batchSize:        opts.batchSize,
		partitionSize:    opts.partitionSize,
		partitionCount:   opts.partitionCount,
		partitionManager: partitions.NewManager(opts.schema),
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

	for i, msg := range msgs {
		if len(msg.Payload) == 0 {
			return fmt.Errorf("payload for topic %q at index %d must not be empty", topic, i)
		}
	}

	wrapped := dbwrap.New(tx, o.schema)
	slug := partitions.TopicSlug(topic)
	seqName := partitions.FillSeqName(slug)

	allocated, err := o.allocateTopicIDs(ctx, wrapped, topic, len(msgs))
	if err != nil {
		return err
	}

	fillHigh, err := o.partitionManager.AdvanceFillSequence(ctx, wrapped, seqName, len(msgs))
	if err != nil {
		return fmt.Errorf("could not advance fill sequence for topic %q: %w", topic, err)
	}

	partitionSize := allocated.PartitionSize
	partitionCount := int(allocated.PartitionCount)
	if nextSize, nextCount, changed := sizing.MaybeResize(topicMetaFromAllocation(allocated), time.Now()); changed {
		if err := o.queries.UpdateTopicSizing(ctx, wrapped, sqlc.UpdateTopicSizingParams{
			Topic:          topic,
			PartitionSize:  nextSize,
			PartitionCount: int32(nextCount),
		}); err != nil {
			return fmt.Errorf("could not update partition sizing for topic %q: %w", topic, err)
		}
		partitionSize = nextSize
		partitionCount = nextCount
	}

	if err := o.partitionManager.EnsureHorizon(
		ctx,
		wrapped,
		o.queries,
		topic,
		slug,
		partitionSize,
		partitionCount,
		allocated.StartID,
		allocated.EndID,
		fillHigh,
	); err != nil {
		return fmt.Errorf("could not ensure partitions for topic %q: %w", topic, err)
	}

	params := make([]sqlc.InsertMessageParams, len(msgs))
	for i, msg := range msgs {
		params[i] = sqlc.InsertMessageParams{
			ID:      allocated.StartID + int64(i),
			Topic:   topic,
			Payload: msg.Payload,
		}
	}

	_, err = o.queries.InsertMessage(ctx, wrapped, params)

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

	holder, err := newLeaseHolder()
	if err != nil {
		return nil, err
	}

	tx, err := o.pool.Begin(ctx)

	if err != nil {
		return nil, fmt.Errorf("could not begin transaction for processing messages for topic %q: %w", topic, err)
	}

	defer tx.Rollback(ctx)

	wrapped := dbwrap.New(tx, o.schema)

	meta, err := o.queries.TryAcquireTopicLease(ctx, wrapped, sqlc.TryAcquireTopicLeaseParams{
		Topic: topic,
		Holder: pgtype.Text{
			String: holder,
			Valid:  true,
		},
	})
	if errors.Is(err, pgx.ErrNoRows) {
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("could not commit skipped processing transaction for topic %q: %w", topic, err)
		}
		return nil, nil
	}

	if err != nil {
		return nil, fmt.Errorf("could not acquire topic lease for %q: %w", topic, err)
	}

	msgs, err := o.queries.ListMessagesAfterAcked(ctx, wrapped, sqlc.ListMessagesAfterAckedParams{
		Topic: topic,
		ID:    meta.AckedID,
		Limit: pgtype.Int4{
			Int32: o.batchSize,
			Valid: true,
		},
	})

	if err != nil {
		return nil, fmt.Errorf("could not list messages for topic %q: %w", topic, err)
	}

	if len(msgs) == 0 {
		if err := o.releaseLease(ctx, wrapped, topic, holder); err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			return nil, fmt.Errorf("could not commit transaction for processing messages for topic %q: %w", topic, err)
		}

		return nil, nil
	}

	highID := msgs[len(msgs)-1].ID
	if err := o.queries.SetTopicLeaseHighID(ctx, wrapped, sqlc.SetTopicLeaseHighIDParams{
		Topic: topic,
		LeaseHighID: pgtype.Int8{
			Int64: highID,
			Valid: true,
		},
		LeaseHolder: pgtype.Text{
			String: holder,
			Valid:  true,
		},
	}); err != nil {
		return nil, fmt.Errorf("could not record leased range for topic %q: %w", topic, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("could not commit lease transaction for topic %q: %w", topic, err)
	}

	if err := f.Flush(ctx, msgs); err != nil {
		_ = o.releaseLeaseInNewTx(ctx, topic, holder)
		return nil, fmt.Errorf("flusher failed for topic %q: %w", topic, err)
	}

	ackTx, err := o.pool.Begin(ctx)

	if err != nil {
		return nil, fmt.Errorf("could not begin ack transaction for topic %q: %w", topic, err)
	}
	defer ackTx.Rollback(ctx)

	ackWrapped := dbwrap.New(ackTx, o.schema)
	if err := o.queries.AckTopicMessages(ctx, ackWrapped, sqlc.AckTopicMessagesParams{
		Topic:           topic,
		AckedID:         highID,
		AcksSinceResize: int64(len(msgs)),
		LeaseHolder: pgtype.Text{
			String: holder,
			Valid:  true,
		},
	}); err != nil {
		return nil, fmt.Errorf("could not ack messages for topic %q: %w", topic, err)
	}

	if err := o.dropAckedPartitions(ctx, ackWrapped, topic, highID); err != nil {
		return nil, err
	}

	if err := ackTx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("could not commit ack transaction for topic %q: %w", topic, err)
	}

	return msgs, nil
}

func (o *outboxImpl) allocateTopicIDs(ctx context.Context, db sqlc.DBTX, topic string, count int) (*sqlc.AllocateTopicIdsRow, error) {
	allocated, err := o.queries.AllocateTopicIds(ctx, db, sqlc.AllocateTopicIdsParams{
		Topic: topic,
		Count: int64(count),
	})
	if err == nil {
		return allocated, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("could not allocate message IDs for topic %q: %w", topic, err)
	}

	if err := o.ensureTopicSetup(ctx, db, topic); err != nil {
		return nil, err
	}

	allocated, err = o.queries.AllocateTopicIds(ctx, db, sqlc.AllocateTopicIdsParams{
		Topic: topic,
		Count: int64(count),
	})
	if err != nil {
		return nil, fmt.Errorf("could not allocate message IDs for topic %q: %w", topic, err)
	}

	return allocated, nil
}

func (o *outboxImpl) ensureTopicSetup(ctx context.Context, db sqlc.DBTX, topic string) error {
	slug := partitions.TopicSlug(topic)
	seqName := partitions.FillSeqName(slug)

	if err := o.partitionManager.LockTopic(ctx, db, topic); err != nil {
		return fmt.Errorf("could not lock topic setup for %q: %w", topic, err)
	}

	if err := o.partitionManager.EnsureFillSequence(ctx, db, seqName); err != nil {
		return fmt.Errorf("could not ensure fill sequence for topic %q: %w", topic, err)
	}

	if err := o.queries.EnsureTopicMeta(ctx, db, sqlc.EnsureTopicMetaParams{
		Topic:          topic,
		PartitionSize:  o.partitionSize,
		PartitionCount: int32(o.partitionCount),
		FillSeqName:    seqName,
	}); err != nil {
		return fmt.Errorf("could not ensure topic metadata for %q: %w", topic, err)
	}

	return nil
}

func (o *outboxImpl) releaseLease(ctx context.Context, db sqlc.DBTX, topic, holder string) error {
	if err := o.queries.ReleaseTopicLease(ctx, db, sqlc.ReleaseTopicLeaseParams{
		Topic: topic,
		LeaseHolder: pgtype.Text{
			String: holder,
			Valid:  true,
		},
	}); err != nil {
		return fmt.Errorf("could not release topic lease for %q: %w", topic, err)
	}
	return nil
}

func (o *outboxImpl) releaseLeaseInNewTx(ctx context.Context, topic, holder string) error {
	tx, err := o.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if err := o.releaseLease(ctx, dbwrap.New(tx, o.schema), topic, holder); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (o *outboxImpl) dropAckedPartitions(ctx context.Context, db sqlc.DBTX, topic string, ackedID int64) error {
	parts, err := o.queries.ListDroppablePartitions(ctx, db, sqlc.ListDroppablePartitionsParams{
		Topic:       topic,
		HighWaterID: ackedID,
	})
	if err != nil {
		return fmt.Errorf("could not list droppable partitions for topic %q: %w", topic, err)
	}

	for _, part := range parts {
		if err := o.partitionManager.DropPartition(ctx, db, part.Relname); err != nil {
			return fmt.Errorf("could not drop partition %q for topic %q: %w", part.Relname, topic, err)
		}
		if err := o.queries.MarkTopicPartitionDropped(ctx, db, sqlc.MarkTopicPartitionDroppedParams{
			Topic:          topic,
			PartitionIndex: part.PartitionIndex,
		}); err != nil {
			return fmt.Errorf("could not mark partition %q dropped for topic %q: %w", part.Relname, topic, err)
		}
	}

	return nil
}

func topicMetaFromAllocation(row *sqlc.AllocateTopicIdsRow) *sqlc.TopicMetum {
	return &sqlc.TopicMetum{
		Topic:             row.Topic,
		NextID:            row.NextID,
		AckedID:           row.AckedID,
		PartitionSize:     row.PartitionSize,
		PartitionCount:    row.PartitionCount,
		FillSeqName:       row.FillSeqName,
		LeaseHolder:       row.LeaseHolder,
		LeaseExpiresAt:    row.LeaseExpiresAt,
		LeaseHighID:       row.LeaseHighID,
		WritesSinceResize: row.WritesSinceResize,
		AcksSinceResize:   row.AcksSinceResize,
		LastWriteAt:       row.LastWriteAt,
		LastProcessAt:     row.LastProcessAt,
		ResizedAt:         row.ResizedAt,
	}
}

func newLeaseHolder() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("could not create lease holder: %w", err)
	}
	return hex.EncodeToString(b[:]), nil
}
