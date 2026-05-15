package pgoutbox

import (
	"context"
	"embed"
	"fmt"
	"io/fs"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
)

//go:embed sqlc/migrations/*.sql
var embedMigrations embed.FS

// schemaNameRE matches a safe Postgres identifier: leading letter or underscore,
// followed by letters, digits, or underscores, up to the 63-byte identifier
// limit. The schema name is interpolated into SQL (CREATE SCHEMA, the goose
// migrations table name, and the search_path startup parameter), so we reject
// anything outside this set to avoid identifier injection.
var schemaNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)

func validateSchemaName(schema string) error {
	if !schemaNameRE.MatchString(schema) {
		return fmt.Errorf("invalid schema name %q: must match %s", schema, schemaNameRE)
	}
	return nil
}

// Migrate runs the embedded pgoutbox migrations against the given pool.
// It is the explicit alternative to NewOutbox's auto-migration: callers
// that want to control when DDL runs (separate startup phase, release
// pipeline, etc.) should construct the outbox with WithAutoMigrate(false)
// and invoke Migrate themselves.
//
// Only WithSchema is consulted from opts; other options are accepted for
// API symmetry but ignored.
func Migrate(ctx context.Context, pool *pgxpool.Pool, opts ...OutboxOpt) error {
	o := defaultOpts()
	for _, f := range opts {
		f(o)
	}
	return runMigrations(ctx, pool, o.schema)
}

func runMigrations(ctx context.Context, pool *pgxpool.Pool, schema string) error {
	if err := validateSchemaName(schema); err != nil {
		return err
	}

	quotedSchema := pgx.Identifier{schema}.Sanitize()

	if _, err := pool.Exec(ctx, "CREATE SCHEMA IF NOT EXISTS "+quotedSchema); err != nil {
		return fmt.Errorf("could not create schema %q: %w", schema, err)
	}

	// Pin search_path on every conn so the unqualified CREATE TABLE in the
	// migration body lands in the configured schema. Safe to use the raw
	// value here because validateSchemaName has already restricted it to
	// identifier-safe characters.
	connConfig := pool.Config().Copy().ConnConfig
	connConfig.RuntimeParams["search_path"] = schema

	db := stdlib.OpenDB(*connConfig)
	defer db.Close()

	fsys, err := fs.Sub(embedMigrations, "sqlc/migrations")
	if err != nil {
		return fmt.Errorf("could not create migration sub-filesystem: %w", err)
	}

	// Instance-based provider — avoids goose's package-level globals so that
	// concurrent migrations against different schemas don't race.
	provider, err := goose.NewProvider(
		goose.DialectPostgres,
		db,
		fsys,
		goose.WithTableName(pgx.Identifier{schema, "migrations"}.Sanitize()),
	)
	if err != nil {
		return fmt.Errorf("could not create migration provider: %w", err)
	}

	if _, err := provider.Up(ctx); err != nil {
		return fmt.Errorf("could not apply migrations: %w", err)
	}

	return nil
}
