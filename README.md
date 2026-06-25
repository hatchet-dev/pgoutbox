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

By default, `NewOutbox` runs migrations and creates an outbox table in the schema `outbox.messages`. This can be overwritten via:

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

## Atomic flush and delete

If your flusher writes to Postgres itself (e.g. into a relay table), implement `TxFlusher` instead of `Flusher`. `ProcessMessages` will pass the same transaction it uses to lock and delete messages, so your writes and the outbox delete commit or roll back together:

```go
type relayFlusher struct{ db *pgxpool.Pool }

func (f *relayFlusher) Flush(_ context.Context, _ []*sqlc.Message) error {
    panic("not used; FlushWithTx is called instead")
}

func (f *relayFlusher) FlushWithTx(ctx context.Context, tx pgx.Tx, msgs []*sqlc.Message) error {
    for _, m := range msgs {
        if _, err := tx.Exec(ctx, "INSERT INTO relay (payload) VALUES ($1)", m.Payload); err != nil {
            return err
        }
    }
    return nil
}
```

## Message expiration

Call `Start` to run background maintenance goroutines that delete old messages. Expiration is configured per topic, with an optional default for topics not explicitly named:

```go
outbox, err := pgoutbox.NewOutbox(pool,
    pgoutbox.WithTopicExpiration("orders", 24*time.Hour),    // orders expire after 24 h
    pgoutbox.WithTopicExpiration("events", 7*24*time.Hour),  // events expire after 7 days
    pgoutbox.WithDefaultExpiration(48*time.Hour),            // all other topics: 48 h
)
if err != nil {
    panic(err)
}

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

outbox.Start(ctx) // returns immediately; goroutines run until ctx is cancelled
```

`Start` is safe to call when no expiration is configured — it becomes a no-op. Topics don't need to be declared at startup: the library tracks every topic that receives a message in a `topics` table (via a Postgres trigger) and applies the default expiration automatically.

Multiple outbox instances (e.g. replicas of the same service) coordinate cleanup using a per-topic maintenance lease, so only one instance runs the delete at a time.

To log errors from the maintenance goroutines, pass a [zerolog](https://github.com/rs/zerolog) logger:

```go
outbox, err := pgoutbox.NewOutbox(pool,
    pgoutbox.WithTopicExpiration("orders", 24*time.Hour),
    pgoutbox.WithLogger(logger),
)
```

Lease competition — another instance winning the cleanup race — is not logged.

## Exclusive consumers

By default, any number of `ProcessMessages` callers can drain a topic concurrently (each call grabs a non-overlapping batch using `FOR UPDATE SKIP LOCKED`). Use `AcquireTopic` when you need exactly one active consumer at a time.

`AcquireTopic` blocks until this instance holds the exclusive lease for the topic, then returns. A background goroutine automatically renews the lease until the context is cancelled, at which point the lease expires and another instance can take over:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

// blocks until the lease is acquired; renews in the background until ctx is done
if err := outbox.AcquireTopic(ctx, "orders"); err != nil {
    panic(err)
}

// only this instance may call ProcessMessages for "orders" while ctx is live
_, err = outbox.ProcessMessages(ctx, "orders")
```

Once a topic has an active exclusive consumer, any other instance calling `ProcessMessages` for that topic receives `pgoutbox.ErrExclusiveLeaseHeld`:

```go
if errors.Is(err, pgoutbox.ErrExclusiveLeaseHeld) {
    // another instance owns this topic right now; skip or retry later
}
```

When the holder's context is cancelled, the lease expires naturally (within the lease duration, 30 s by default) and another instance's `AcquireTopic` call unblocks.

## Benchmarks

You can run benchmarks locally; for example, to write and flush 100k messages, you can run:

```
go test -bench=. -benchtime=100000x
```

On a local Macbook with an M3 Max core, this results in `8492 msgs/sec`:

```
$ go test -bench=. -benchtime=100000x
goos: darwin
goarch: arm64
pkg: github.com/hatchet-dev/pgoutbox
cpu: Apple M3 Max
BenchmarkOutbox_WriteAndPublishThroughput-14              100000            117757 ns/op              8492 msgs/sec
```
