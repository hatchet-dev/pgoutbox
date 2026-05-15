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
	const (
		numWorkers  = MAX_CONNS
		maxInFlight = 5000
		batchSize   = 500
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	schema := uniqueSchema(b)

	outbox, err := pgoutbox.NewOutbox(
		sharedPool,
		pgoutbox.WithSchema(schema),
		pgoutbox.WithBatchSize(batchSize),
	)
	require.NoError(b, err)

	// inFlight bounds the number of un-flushed rows allowed in the table at
	// once: producers acquire a slot before INSERT, the flusher releases it
	// after the row is committed-deleted.
	inFlight := make(chan struct{}, maxInFlight)
	var flushed atomic.Int64

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
