package pgoutbox_test

import (
	"context"
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

func BenchmarkOutbox_WriteAndPublishThroughput(b *testing.B) {
	b.Run("Flush", func(b *testing.B) {
		benchmarkThroughput(b, func(_ string, onFlush func(int)) pgoutbox.Flusher {
			return &countingFlusher{onFlush: onFlush}
		})
	})
	b.Run("TxFlush", func(b *testing.B) {
		benchmarkThroughput(b, func(schema string, onFlush func(int)) pgoutbox.Flusher {
			table := pgx.Identifier{schema, "bench_side_log"}.Sanitize()
			_, err := sharedPool.Exec(context.Background(), fmt.Sprintf("CREATE TABLE %s (msg_id bigint NOT NULL)", table))
			require.NoError(b, err)
			return &txCountingFlusher{table: table, onFlush: onFlush}
		})
	})
}

// benchmarkThroughput drives a producer/consumer loop against a freshly-built
// outbox. newFlusher builds the flusher under test from the test schema and the
// inFlight-releasing callback, letting us reuse the harness for both the plain
// Flush path and the FlushWithTx path.
func benchmarkThroughput(b *testing.B, newFlusher func(schema string, onFlush func(n int)) pgoutbox.Flusher) {
	const (
		numWorkers  = MAX_CONNS
		maxInFlight = 5000
		batchSize   = 500
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	schema := uniqueSchema(b)

	outbox, err := pgoutbox.NewOutbox(
		ctx,
		sharedPool,
		pgoutbox.WithSchema(schema),
	)
	require.NoError(b, err)

	// inFlight bounds the number of un-flushed rows allowed in the table at
	// once: producers acquire a slot before INSERT, the flusher releases it
	// after the row is committed-deleted.
	inFlight := make(chan struct{}, maxInFlight)
	var flushed atomic.Int64

	outbox.AddFlusher("bench", newFlusher(schema, func(n int) {
		for range n {
			<-inFlight
		}
		flushed.Add(int64(n))
	}))

	procCtx, stopProcessor := context.WithCancel(ctx)
	var procWg sync.WaitGroup
	procWg.Go(func() {
		for procCtx.Err() == nil {
			msgs, err := outbox.ProcessMessages(procCtx, "bench", pgoutbox.WithBatchSize(batchSize))
			if err != nil {
				if procCtx.Err() != nil {
					return
				}
				b.Logf("ProcessMessages: %v", err)
				continue
			}
			if len(msgs) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	})

	payload := []byte(`{"id":1}`)

	b.ResetTimer()

	work := make(chan struct{})
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(func() {
			for range work {
				inFlight <- struct{}{}

				tx, err := sharedPool.Begin(ctx)
				if err != nil {
					b.Errorf("begin: %v", err)
					return
				}
				if err := outbox.AddMessages(ctx, tx, "bench", []pgoutbox.MessageOpts{
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
		time.Sleep(time.Millisecond)
	}

	b.StopTimer()

	stopProcessor()
	procWg.Wait()

	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(b.N)/elapsed, "msgs/sec")
}
