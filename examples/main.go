package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/hatchet-dev/pgoutbox"
	"github.com/hatchet-dev/pgoutbox/sqlc"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	schema = "demo"
	topic  = "orders"
)

type printFlusher struct{}

func (printFlusher) Flush(_ context.Context, msgs []*sqlc.Message) error {
	for _, m := range msgs {
		fmt.Printf("  flushed id=%d topic=%s payload=%s\n", m.ID, m.Topic, string(m.Payload))
	}
	return nil
}

func main() {
	ctx := context.Background()

	databaseUrl := os.Getenv("DATABASE_URL")
	if databaseUrl == "" {
		databaseUrl = "postgresql://postgres:postgres@localhost:5445/outbox_demo?sslmode=disable"
	}

	// Caller pool: notice it has NO search_path override. The outbox rewrites
	// schema-templated SQL on the wire, so writes still land in "demo".messages.
	pool, err := pgxpool.New(ctx, databaseUrl)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	outbox, err := pgoutbox.NewOutbox(pool, pgoutbox.WithSchema(schema))
	if err != nil {
		log.Fatalf("create outbox: %v", err)
	}
	outbox.AddFlusher(topic, printFlusher{})

	tx, err := pool.Begin(ctx)
	if err != nil {
		log.Fatalf("begin: %v", err)
	}
	defer tx.Rollback(ctx)

	msgs := []pgoutbox.MessageOpts{
		{Payload: mustJSON(map[string]any{"order_id": 1, "amount": 42.50})},
		{Payload: mustJSON(map[string]any{"order_id": 2, "amount": 9.99})},
		{Payload: mustJSON(map[string]any{"order_id": 3, "amount": 100.00})},
	}

	if err := outbox.AddMessages(ctx, tx, topic, msgs); err != nil {
		log.Fatalf("add: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("commit: %v", err)
	}

	fmt.Printf("staged %d messages on topic %q in schema %q\n", len(msgs), topic, schema)

	fmt.Println("processing...")
	if _, err := outbox.ProcessMessages(ctx, topic); err != nil {
		log.Fatalf("process: %v", err)
	}
	fmt.Println("done")
}

func mustJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	return b
}
