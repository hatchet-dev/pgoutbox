package pgoutbox

import (
	"context"

	"github.com/hatchet-dev/pgoutbox/sqlc"
)

type NopFlusher struct{}

func NewNopFlusher() *NopFlusher {
	return &NopFlusher{}
}

func (f *NopFlusher) Flush(ctx context.Context, msgs []*sqlc.Message) error {
	return nil
}
