package pgoutbox

import "github.com/hatchet-dev/pgoutbox/sqlc"

type NopFlusher struct{}

func NewNopFlusher() *NopFlusher {
	return &NopFlusher{}
}

func (f *NopFlusher) Flush(_ FlushContext, _ []*sqlc.Message) error {
	return nil
}
