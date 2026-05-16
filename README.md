# `pgoutbox` - a transactional outbox for `pgx`

`pgoutbox` implements a simple [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) for [`pgx`](https://github.com/jackc/pgx). New messages can be added to a Postgres table using `AddMessages` and can be flushed to a destination via `ProcessMessages`.

Here's an example of flushing messages on `topic1` by simply printing them to the console:

```go
type printFlusher struct{}

func (printFlusher) Flush(_ context.Context, msgs []*sqlc.Message) error {
	for _, m := range msgs {
		fmt.Printf("  flushed id=%d topic=%s payload=%s\n", m.ID, m.Topic, string(m.Payload))
	}
	return nil
}

outbox, err := pgoutbox.NewOutbox(pool)

if err != nil {
    panic(err)
}

outbox.AddFlusher("topic1", printFlusher{})
```

Then, within a transaction, messages can be added via:

```go
if err := outbox.AddMessages(ctx, tx, "topic1", msgs); err != nil {
    panic(err)
}
```

And messages can be flushed after the transaction commits using:

```go
_, err := outbox.ProcessMessages(ctx, "topic1");

if err != nil {
    panic(err)
}
```

## Schema

By default, `NewOutbox` runs migrations and creates partitioned outbox tables in the `outbox` schema. `messages` is partitioned by topic and then by per-topic ID ranges; flushed rows are append-only until a sealed, fully-acked partition can be dropped. The schema can be overwritten via:

```go
outbox, err := pgoutbox.NewOutbox(pool, pgoutbox.WithSchema("my_schema"))
```

If you'd rather run migrations yourself (for example, as part of a separate release step), disable the auto-migration and invoke `Migrate` explicitly:

```go
outbox, err := pgoutbox.NewOutbox(pool,
    pgoutbox.WithSchema("my_schema"),
    pgoutbox.WithAutoMigrate(false),
)

if err := pgoutbox.Migrate(ctx, pool, pgoutbox.WithSchema("my_schema")); err != nil {
    panic(err)
}
```

Partition sizing defaults to 100,000 rows per range partition and two future partitions per topic. Both values are persisted in `topic_meta` per topic and can be configured for newly-created topics:

```go
outbox, err := pgoutbox.NewOutbox(pool,
    pgoutbox.WithDefaultPartitionSize(500_000),
    pgoutbox.WithDefaultPartitionCount(4),
)
```

The outbox also keeps an approximate sequence-backed fill counter for each topic. It is used only to create future partitions early and to guide sizing; delivery safety is governed by the transactional per-topic ID cursor and `acked_id`.

## Multiple topics and flushers

It's easy to configure multiple destinations using topics registered for each flusher:

```go
outbox.AddFlusher("orders", ordersFlusher{})
outbox.AddFlusher("shipments", shipmentsFlusher{})

// within a single transaction, write to whichever topics you need
tx, err := pool.Begin(ctx)
if err != nil {
    panic(err)
}

if err := outbox.AddMessages(ctx, tx, "orders", orderMsgs); err != nil {
    panic(err)
}
if err := outbox.AddMessages(ctx, tx, "shipments", shipmentMsgs); err != nil {
    panic(err)
}

if err := tx.Commit(ctx); err != nil {
    panic(err)
}

// each topic is drained independently by its registered flusher
outbox.ProcessMessages(ctx, "orders")
outbox.ProcessMessages(ctx, "shipments")
```

## Benchmarks

You can run benchmarks locally; for example, to write and flush 100k messages without also running the test suite, you can run:

```
go test -run '^$' -bench=. -benchtime=100000x
```

On a local Macbook with an M3 Max core, older non-partitioned versions measured around `8492 msgs/sec`; current benchmark output includes both default partitioning and frequent-rollover partitioning cases:

```
$ go test -run '^$' -bench=. -benchtime=100000x
goos: darwin
goarch: arm64
pkg: github.com/hatchet-dev/pgoutbox
cpu: Apple M3 Max
BenchmarkOutbox_WriteAndPublishThroughput/DefaultPartitions-14
BenchmarkOutbox_WriteAndPublishThroughput/FrequentPartitionRollover-14
```
