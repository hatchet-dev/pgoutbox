package pgoutbox_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/pgoutbox"
	"github.com/hatchet-dev/pgoutbox/sqlc"
)

type countingFlusher struct {
	onFlush func(n int)
}

func (c *countingFlusher) Flush(_ pgoutbox.FlushContext, msgs []*sqlc.Message) error {
	if c.onFlush != nil {
		c.onFlush(len(msgs))
	}
	return nil
}

// txCountingFlusher writes one row per message through the transaction exposed
// by the FlushContext (so the work is comparable to a real tx-bound flush)
// before running the same bookkeeping callback.
type txCountingFlusher struct {
	table   string // schema-qualified, sanitized
	onFlush func(n int)
}

func (c *txCountingFlusher) Flush(ctx pgoutbox.FlushContext, msgs []*sqlc.Message) error {
	tx := ctx.Tx()
	for _, m := range msgs {
		if _, err := tx.Exec(ctx, fmt.Sprintf("INSERT INTO %s (msg_id) VALUES ($1)", c.table), m.ID); err != nil {
			return err
		}
	}
	if c.onFlush != nil {
		c.onFlush(len(msgs))
	}
	return nil
}

// benchTopicCounts is the topic-count dimension of the benchmark matrix:
// producers spray messages round-robin across the topics and one consumer
// drains each topic.
var benchTopicCounts = []int{1, 10}

func BenchmarkOutbox_WriteAndPublishThroughput(b *testing.B) {
	benchmarkThroughputMatrix(b, false)
}

// BenchmarkOutbox_SubscribeThroughput is the notification-driven counterpart
// of BenchmarkOutbox_WriteAndPublishThroughput: producers stage messages as
// fast as they can (each commit publishing a pg_notify via the attached
// PGPubSub) while one Subscribe call per topic drains it. The poll interval
// is set far above the benchmark's runtime, so throughput here is carried by
// the LISTEN/NOTIFY wake-ups, not polling.
func BenchmarkOutbox_SubscribeThroughput(b *testing.B) {
	benchmarkThroughputMatrix(b, true)
}

// benchmarkThroughputMatrix runs the Flush/TxFlush × topic-count grid.
func benchmarkThroughputMatrix(b *testing.B, useSubscribe bool) {
	for _, numTopics := range benchTopicCounts {
		b.Run(fmt.Sprintf("Flush/topics=%d", numTopics), func(b *testing.B) {
			benchmarkThroughput(b, func(_ string, onFlush func(int)) pgoutbox.Flusher {
				return &countingFlusher{onFlush: onFlush}
			}, useSubscribe, numTopics)
		})
		b.Run(fmt.Sprintf("TxFlush/topics=%d", numTopics), func(b *testing.B) {
			benchmarkThroughput(b, func(schema string, onFlush func(int)) pgoutbox.Flusher {
				table := pgx.Identifier{schema, "bench_side_log"}.Sanitize()
				_, err := sharedPool.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (msg_id bigint NOT NULL)", table))
				require.NoError(b, err)
				return &txCountingFlusher{table: table, onFlush: onFlush}
			}, useSubscribe, numTopics)
		})
	}
}

// benchmarkThroughput drives a producer/consumer loop against a freshly-built
// outbox. newFlusher builds the flusher under test from the test schema and the
// inFlight-releasing callback, letting us reuse the harness for both the plain
// Flush path and the FlushWithTx path. With useSubscribe each topic's consumer
// is a Subscribe call woken by pg_notify (and AddMessages pays the in-tx
// publish); otherwise it is a busy-polling ProcessMessages loop. Producers
// assign messages to the numTopics topics round-robin.
func benchmarkThroughput(b *testing.B, newFlusher func(schema string, onFlush func(n int)) pgoutbox.Flusher, useSubscribe bool, numTopics int) {
	const (
		numWorkers  = MAX_CONNS
		maxInFlight = 5000
		batchSize   = 500
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	schema := uniqueSchema(b)

	opts := []pgoutbox.OutboxOpt{pgoutbox.WithSchema(schema)}
	if useSubscribe {
		// The schema name doubles as the NOTIFY channel so concurrent
		// benchmarks don't wake each other's subscribers.
		ps, err := pgoutbox.NewPGPubSub(ctx, sharedPool, pgoutbox.WithNotifyChannel(schema))
		require.NoError(b, err)
		opts = append(opts, pgoutbox.WithPubSub(ps))
	}

	outbox, err := pgoutbox.NewOutbox(ctx, sharedPool, opts...)
	require.NoError(b, err)

	topics := make([]string, numTopics)
	for i := range topics {
		topics[i] = fmt.Sprintf("bench_%d", i)
	}

	// inFlight bounds the number of un-flushed rows allowed in the table at
	// once: producers acquire a slot before INSERT, the flusher releases it
	// after the row is committed-deleted.
	inFlight := make(chan struct{}, maxInFlight)
	var flushed atomic.Int64

	flusher := newFlusher(schema, func(n int) {
		for range n {
			<-inFlight
		}
		flushed.Add(int64(n))
	})
	for _, topic := range topics {
		outbox.AddFlusher(topic, flusher)
	}

	procCtx, stopProcessor := context.WithCancel(ctx)
	var procWg sync.WaitGroup
	for _, topic := range topics {
		if useSubscribe {
			procWg.Go(func() {
				// The poll interval is a safety net well above the benchmark's
				// runtime: every drain below must be triggered by a notification.
				err := outbox.Subscribe(procCtx, topic,
					pgoutbox.WithPollInterval(30*time.Second),
					pgoutbox.WithProcessOpts(pgoutbox.WithBatchSize(batchSize)),
				)
				if err != nil && !errors.Is(err, context.Canceled) {
					b.Errorf("Subscribe %s: %v", topic, err)
				}
			})
		} else {
			procWg.Go(func() {
				for procCtx.Err() == nil {
					msgs, err := outbox.ProcessMessages(procCtx, topic, pgoutbox.WithBatchSize(batchSize))
					if err != nil {
						if procCtx.Err() != nil {
							return
						}
						b.Logf("ProcessMessages %s: %v", topic, err)
						continue
					}
					if len(msgs) == 0 {
						time.Sleep(time.Millisecond)
					}
				}
			})
		}
	}

	payload := []byte(`{"id":1}`)

	b.ResetTimer()

	work := make(chan struct{})
	var msgIdx atomic.Int64
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(func() {
			for range work {
				inFlight <- struct{}{}
				topic := topics[msgIdx.Add(1)%int64(numTopics)]

				tx, err := sharedPool.Begin(ctx)
				if err != nil {
					b.Errorf("begin: %v", err)
					return
				}
				if err := outbox.AddMessages(ctx, tx, topic, []pgoutbox.MessageOpts{
					{Payload: payload},
				}); err != nil {
					_ = tx.Rollback(ctx)
					b.Errorf("AddMessages: %v", err)
					return
				}
				if err := tx.Commit(ctx); err != nil {
					b.Errorf("commit: %v", err)
					return
				}
			}
		})
	}

	for range b.N {
		work <- struct{}{}
	}
	close(work)
	wg.Wait()

	for flushed.Load() < int64(b.N) {
		if ctx.Err() != nil || b.Failed() {
			b.Fatalf("consumer stalled: flushed %d of %d messages", flushed.Load(), b.N)
		}
		time.Sleep(time.Millisecond)
	}

	b.StopTimer()

	stopProcessor()
	procWg.Wait()

	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(b.N)/elapsed, "msgs/sec")
}
