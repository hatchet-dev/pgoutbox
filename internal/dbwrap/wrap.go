package dbwrap

import (
	"context"
	"regexp"

	"github.com/hatchet-dev/pgoutbox/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var tmplRE = regexp.MustCompile(`/\*tmpl\*/\s*(\S+?)\s*/\*tmpl\*/`)

type Wrapper struct {
	dbtx       sqlc.DBTX
	schemaQual string
	schemaRaw  string
}

func New(dbtx sqlc.DBTX, schema string) *Wrapper {
	return &Wrapper{
		dbtx:       dbtx,
		schemaQual: pgx.Identifier{schema}.Sanitize() + ".",
		schemaRaw:  schema,
	}
}

func (w *Wrapper) rewrite(sql string) string {
	return tmplRE.ReplaceAllString(sql, w.schemaQual+"$1")
}

func (w *Wrapper) Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	return w.dbtx.Exec(ctx, w.rewrite(sql), args...)
}

func (w *Wrapper) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return w.dbtx.Query(ctx, w.rewrite(sql), args...)
}

func (w *Wrapper) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return w.dbtx.QueryRow(ctx, w.rewrite(sql), args...)
}

func (w *Wrapper) CopyFrom(ctx context.Context, table pgx.Identifier, cols []string, src pgx.CopyFromSource) (int64, error) {
	qualified := append(pgx.Identifier{w.schemaRaw}, table...)
	return w.dbtx.CopyFrom(ctx, qualified, cols, src)
}
