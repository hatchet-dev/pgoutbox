# `pgoutbox` - a transactional outbox for `pgx`

`pgoutbox` implements a simple [transactional outbox](https://microservices.io/patterns/data/transactional-outbox.html) for [`pgx`](https://github.com/jackc/pgx). New messages can be added to a Postgres table using `AddMessages` and can be flushed to a destination via `ProcessMessages`.

Here's an example of flushing messages on `topic1` by simply printing them to the console:

```go
type printFlusher struct{}

func (printFlusher) Flush(_ pgoutbox.FlushContext, msgs []*sqlc.Message) error {
	for _, m := range msgs {
		fmt.Printf("  flushed id=%d topic=%s payload=%s\n", m.ID, m.Topic, string(m.Payload))
	}
	return nil
}

ctx, cancel := context.WithCancel(context.Background())
defer cancel()

outbox, err := pgoutbox.NewOutbox(ctx, pool)

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
outbox, err := pgoutbox.NewOutbox(ctx, pool, pgoutbox.WithSchema("my_schema"))
```

If you'd rather run migrations yourself (for example, as part of a separate release step), disable the auto-migration and invoke `Migrate` explicitly:

```go
outbox, err := pgoutbox.NewOutbox(ctx, pool,
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

If your flusher writes to Postgres itself (e.g. into a relay table), use the transaction exposed by the `FlushContext` passed to `Flush`. It is the same transaction `ProcessMessages` uses to lock and delete messages, so your writes and the outbox delete commit or roll back together:

```go
type relayFlusher struct{}

func (f *relayFlusher) Flush(ctx pgoutbox.FlushContext, msgs []*sqlc.Message) error {
    tx := ctx.Tx()
    for _, m := range msgs {
        if _, err := tx.Exec(ctx, "INSERT INTO relay (payload) VALUES ($1)", m.Payload); err != nil {
            return err
        }
    }
    return nil
}
```

`FlushContext` embeds `context.Context`, so flushers that don't need the transaction can ignore `Tx()` and treat it as a plain context.

## Continuous processing with `Subscribe`

Instead of calling `ProcessMessages` yourself, `Subscribe` runs it in a loop: it drains the topic, then waits until either the poll interval elapses or a new-message notification arrives (see below), and drains again. It blocks until its context is cancelled:

```go
go func() {
    err := outbox.Subscribe(ctx, "orders",
        pgoutbox.WithPollInterval(5*time.Second),              // default: 5s
        pgoutbox.WithProcessOpts(pgoutbox.WithBatchSize(500)), // forwarded to every ProcessMessages call
    )
    if err != nil && !errors.Is(err, context.Canceled) {
        panic(err)
    }
}()
```

Processing errors don't kill the loop — they're logged to the `WithLogger` logger and retried on the next wake-up. `Subscribe` returns an error immediately only if no flusher is registered for the topic or the subscription itself can't be established.

### Waking on new messages with `LISTEN`/`NOTIFY`

Without further configuration, `Subscribe` is purely poll-based. To wake subscribers the moment new messages commit, attach a `PubSub` — the built-in implementation rides Postgres `LISTEN`/`NOTIFY`:

```go
ps, err := pgoutbox.NewPGPubSub(ctx, pool)
if err != nil {
    panic(err)
}

outbox, err := pgoutbox.NewOutbox(ctx, pool, pgoutbox.WithPubSub(ps))
```

With a `PubSub` attached, `AddMessages` publishes a notification for each staged topic and `Subscribe` wakes on it instead of waiting out the poll interval. The pg-backed `PubSub` publishes *inside the `AddMessages` transaction*, so the notification is delivered exactly when the insert commits — and never for a transaction that rolls back. Postgres deduplicates identical notifications within a transaction, so any number of `AddMessages` calls for a topic in one transaction cost a single wake-up.

Delivery is best-effort by design: if a notification is lost (for example while the listener reconnects), polling picks the messages up within one poll interval. The listener occupies a single dedicated connection (hijacked out of the pool so it doesn't consume a pool slot) no matter how many topics are subscribed.

Two details worth knowing:

- All notifications travel over one NOTIFY channel, `pgoutbox_pubsub` by default. Two outboxes sharing a database (e.g. different schemas) should use distinct channels via `pgoutbox.WithNotifyChannel("my_channel")` to avoid waking each other's subscribers.
- `PubSub` is an interface, so you can bring your own transport (e.g. Redis, NATS) instead of `LISTEN`/`NOTIFY`. If your implementation also implements `TxPublisher`, notifications are published transactionally as described above; otherwise `AddMessages` publishes best-effort at insert time.

## Message expiration

`NewOutbox` starts background maintenance goroutines that delete old messages; they run until the context passed to `NewOutbox` is cancelled. Expiration is configured per topic, with an optional default for topics not explicitly named:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

outbox, err := pgoutbox.NewOutbox(ctx, pool,
    pgoutbox.WithTopicExpiration("orders", 24*time.Hour),    // orders expire after 24 h
    pgoutbox.WithTopicExpiration("events", 7*24*time.Hour),  // events expire after 7 days
    pgoutbox.WithDefaultExpiration(48*time.Hour),            // all other topics: 48 h
)
if err != nil {
    panic(err)
}
```

The outbox always runs a background scanner goroutine that polls the `topics` table, but maintenance loops are only launched for topics that actually have an expiration configured. Topics don't need to be declared at startup: the library tracks every topic that receives a message in a `topics` table (via a Postgres trigger) and applies the default expiration automatically.

Multiple outbox instances (e.g. replicas of the same service) coordinate cleanup using a per-topic maintenance lease, so only one instance runs the delete at a time.

To log errors from the maintenance goroutines, pass a [zerolog](https://github.com/rs/zerolog) logger:

```go
outbox, err := pgoutbox.NewOutbox(ctx, pool,
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

An instance that never acquired the lease (or whose lease has expired) receives `pgoutbox.ErrExclusiveLeaseRequired` instead.

When the holder's context is cancelled, the lease expires naturally (within the lease duration, 30 s by default) and another instance's `AcquireTopic` call unblocks.

### Exclusive subscribers

`Subscribe` composes with exclusive consumers: pass `WithExclusive()` and it manages the lease for you.

```go
// Exactly one instance across the fleet drains "orders" at a time; the rest
// wait in line and take over on failure.
err := outbox.Subscribe(ctx, "orders", pgoutbox.WithExclusive())
```

With `WithExclusive()`, `Subscribe`:

1. acquires the lease before its first processing pass, blocking while another instance holds it (like `AcquireTopic`);
2. re-acquires it automatically if the lease is ever lost mid-subscribe (for example, a heartbeat lapse during a database blip); and
3. releases it on return, so a waiting instance takes over immediately instead of waiting out the lease's grace period.

Several instances calling `Subscribe(..., WithExclusive())` on the same topic therefore form a failover group: one active consumer, the rest hot standbys. Nothing is missed during a handoff — the new holder's first drain pass covers any backlog that accumulated while the lease changed hands.

Without `WithExclusive()`, subscribing to a topic whose exclusive lease is held elsewhere doesn't fail — every pass errors (visible via `WithLogger`) and is retried, so the subscriber sits idle until the lease frees up or is acquired. For an exclusive topic, either call `AcquireTopic` before `Subscribe` or pass `WithExclusive()`.

## Benchmarks

You can run benchmarks locally; for example, to write and flush 100k messages, you can run:

```
go test -bench=. -benchtime=100000x
```

`BenchmarkOutbox_WriteAndPublishThroughput` drains each topic with a busy-polling `ProcessMessages` loop; `BenchmarkOutbox_SubscribeThroughput` drains each topic with a `Subscribe` call woken by `LISTEN`/`NOTIFY` (its poll interval is set far above the benchmark runtime, so throughput is carried entirely by notifications — and each producer commit pays the in-transaction `pg_notify`). Both run at 1 and 10 topics, with producers spreading messages round-robin and one consumer per topic. On a local Macbook with an M3 Max core:

```
$ go test -bench=. -benchtime=100000x
goos: darwin
goarch: arm64
pkg: github.com/hatchet-dev/pgoutbox
cpu: Apple M3 Max
BenchmarkOutbox_WriteAndPublishThroughput/Flush/topics=1-14         	  100000	    139432 ns/op	      7172 msgs/sec
BenchmarkOutbox_WriteAndPublishThroughput/TxFlush/topics=1-14       	  100000	    248696 ns/op	      4021 msgs/sec
BenchmarkOutbox_WriteAndPublishThroughput/Flush/topics=10-14        	  100000	    245562 ns/op	      4072 msgs/sec
BenchmarkOutbox_WriteAndPublishThroughput/TxFlush/topics=10-14      	  100000	    242325 ns/op	      4127 msgs/sec
BenchmarkOutbox_SubscribeThroughput/Flush/topics=1-14               	  100000	    188324 ns/op	      5310 msgs/sec
BenchmarkOutbox_SubscribeThroughput/TxFlush/topics=1-14             	  100000	    305362 ns/op	      3275 msgs/sec
BenchmarkOutbox_SubscribeThroughput/Flush/topics=10-14              	  100000	    301837 ns/op	      3313 msgs/sec
BenchmarkOutbox_SubscribeThroughput/TxFlush/topics=10-14            	  100000	    321406 ns/op	      3111 msgs/sec
```
