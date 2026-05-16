package pgoutbox_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/hatchet-dev/pgoutbox"
	"github.com/hatchet-dev/pgoutbox/sqlc"
)

type countingFlusher struct {
	onFlush func(n int)
}

func (c *countingFlusher) Flush(_ context.Context, msgs []*sqlc.Message) error {
	if c.onFlush != nil {
		c.onFlush(len(msgs))
	}
	return nil
}

func BenchmarkOutbox_WriteAndPublishThroughput(b *testing.B) {
	b.Run("DefaultPartitions", func(b *testing.B) {
		benchmarkOutboxWriteAndPublishThroughput(b)
	})
	b.Run("FrequentPartitionRollover", func(b *testing.B) {
		benchmarkOutboxWriteAndPublishThroughput(
			b,
			pgoutbox.WithDefaultPartitionSize(1000),
			pgoutbox.WithDefaultPartitionCount(4),
		)
	})
}

func benchmarkOutboxWriteAndPublishThroughput(b *testing.B, opts ...pgoutbox.OutboxOpt) {
	const (
		numWorkers  = MAX_CONNS
		maxInFlight = 5000
		batchSize   = 500
		writeBatch  = 100
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	schema := uniqueSchema(b)

	outbox, err := pgoutbox.NewOutbox(
		sharedPool,
		append([]pgoutbox.OutboxOpt{
			pgoutbox.WithSchema(schema),
			pgoutbox.WithBatchSize(batchSize),
		}, opts...)...,
	)
	require.NoError(b, err)

	// inFlight bounds the number of un-flushed rows allowed in the table at
	// once: producers acquire a slot before INSERT, the flusher releases it
	// after the row is flushed and the per-topic ack cursor advances.
	inFlight := make(chan struct{}, maxInFlight)
	errCh := make(chan error, 1)
	var flushed atomic.Int64
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}

	outbox.AddFlusher("bench", &countingFlusher{
		onFlush: func(n int) {
			for range n {
				<-inFlight
			}
			flushed.Add(int64(n))
		},
	})

	procCtx, stopProcessor := context.WithCancel(ctx)
	var procWg sync.WaitGroup
	procWg.Go(func() {
		for procCtx.Err() == nil {
			msgs, err := outbox.ProcessMessages(procCtx, "bench")
			if err != nil {
				if procCtx.Err() != nil {
					return
				}
				reportErr(err)
				return
			}
			if len(msgs) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	})

	payload := []byte(`{"id":1}`)

	b.ResetTimer()

	work := make(chan int)
	var wg sync.WaitGroup
	for range numWorkers {
		wg.Go(func() {
			for n := range work {
				for range n {
					inFlight <- struct{}{}
				}

				tx, err := sharedPool.Begin(ctx)
				if err != nil {
					reportErr(err)
					return
				}
				msgs := make([]pgoutbox.MessageOpts, n)
				for i := range msgs {
					msgs[i] = pgoutbox.MessageOpts{Payload: payload}
				}
				if err := outbox.AddMessages(ctx, tx, "bench", msgs); err != nil {
					_ = tx.Rollback(ctx)
					reportErr(err)
					return
				}
				if err := tx.Commit(ctx); err != nil {
					reportErr(err)
					return
				}
			}
		})
	}

	for remaining := b.N; remaining > 0; {
		n := writeBatch
		if remaining < n {
			n = remaining
		}
		select {
		case err := <-errCh:
			close(work)
			wg.Wait()
			stopProcessor()
			procWg.Wait()
			b.Fatalf("benchmark worker failed: %v", err)
		case work <- n:
			remaining -= n
		}
	}
	close(work)
	wg.Wait()

	for flushed.Load() < int64(b.N) {
		select {
		case err := <-errCh:
			stopProcessor()
			procWg.Wait()
			b.Fatalf("benchmark processor failed: %v", err)
		case <-ctx.Done():
			stopProcessor()
			procWg.Wait()
			b.Fatalf("timed out waiting for flush progress: flushed=%d want=%d: %v", flushed.Load(), b.N, ctx.Err())
		default:
			time.Sleep(time.Millisecond)
		}
	}

	b.StopTimer()

	stopProcessor()
	procWg.Wait()

	elapsed := b.Elapsed().Seconds()
	b.ReportMetric(float64(b.N)/elapsed, "msgs/sec")
}
