// Package harness provides reusable scaffolding for the pgoutbox e2e tests.
// It exposes helpers for spinning up real backing services (currently postgres)
// inside testcontainers.
package harness

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

// DefaultPostgresVersion is the postgres image tag used when callers do not
// pin a version explicitly.
const DefaultPostgresVersion = "18-alpine"

// PostgresOptions configures the postgres container started by Start.
type PostgresOptions struct {
	// Version is the postgres image tag (e.g. "16-alpine"). Defaults to
	// DefaultPostgresVersion when empty.
	Version string
	// Database, Username, Password control the credentials seeded into the
	// container. Defaults match the testcontainers-go postgres module.
	Database string
	Username string
	Password string
}

func (o PostgresOptions) withDefaults() PostgresOptions {
	if o.Version == "" {
		o.Version = DefaultPostgresVersion
	}
	if o.Database == "" {
		o.Database = "test"
	}
	if o.Username == "" {
		o.Username = "user"
	}
	if o.Password == "" {
		o.Password = "password"
	}
	return o
}

// Start boots a postgres container with default settings. The returned cleanup
// function terminates the container; callers are responsible for invoking it.
func Start(ctx context.Context) (string, func(), error) {
	return StartWith(ctx, PostgresOptions{})
}

// StartWith boots a postgres container using the supplied options and returns
// its connection string (with sslmode=disable) along with a cleanup function.
func StartWith(ctx context.Context, opts PostgresOptions) (string, func(), error) {
	opts = opts.withDefaults()

	container, err := postgres.Run(
		ctx,
		fmt.Sprintf("postgres:%s", opts.Version),
		postgres.WithDatabase(opts.Database),
		postgres.WithUsername(opts.Username),
		postgres.WithPassword(opts.Password),
	)
	if err != nil {
		return "", nil, fmt.Errorf("start postgres container: %w", err)
	}

	cleanup := func() {
		termCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = container.Terminate(termCtx)
	}

	connStr, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("get postgres connection string: %w", err)
	}

	if err := waitForPostgres(ctx, connStr); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("postgres never became ready: %w", err)
	}

	return connStr, cleanup, nil
}

func waitForPostgres(ctx context.Context, connStr string) error {
	const attempts = 10
	var lastErr error

	for i := 0; i < attempts; i++ {
		conn, err := pgx.Connect(ctx, connStr)
		if err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}
		if err := conn.Ping(ctx); err != nil {
			lastErr = err
			_ = conn.Close(ctx)
			time.Sleep(2 * time.Second)
			continue
		}
		_ = conn.Close(ctx)
		return nil
	}

	return fmt.Errorf("failed to connect after %d attempts: %w", attempts, lastErr)
}
