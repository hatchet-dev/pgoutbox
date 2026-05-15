# pgoutbox example

A minimal end-to-end demonstration of `pgoutbox`:

- Start Postgres in Docker.
- Stage three messages on the `orders` topic inside a transaction.
- Process the topic — a `printFlusher` prints each message as it's flushed, and the rows are deleted after a successful flush.

The example writes through a `pgxpool.Pool` with no `search_path` override. Schema qualification is handled inside the outbox via inline SQL rewriting, so writes land in `"demo".messages` even though the caller's connection knows nothing about the outbox schema.

## Prerequisites

- Docker (for the `postgres:16-alpine` container)
- Go 1.25+

## Run

```sh
# 1. Bring up Postgres on localhost:5432
docker compose up -d

# 2. Run the example
go run .

# 3. Tear down
docker compose down
```

Expected output:

```
staged 3 messages on topic "orders" in schema "demo"
processing...
  flushed id=1 topic=orders payload={"amount":42.5,"order_id":1}
  flushed id=2 topic=orders payload={"amount":9.99,"order_id":2}
  flushed id=3 topic=orders payload={"amount":100,"order_id":3}
done
```

## Configuration

Override the connection string with `DATABASE_URL`:

```sh
DATABASE_URL=postgresql://user:pass@host:5432/dbname?sslmode=disable go run .
```
