package pgoutbox

import "github.com/hatchet-dev/pgoutbox/sqlc"

// NopFlusher is a Flusher that discards every message it is given. It is useful
// in tests, and for draining a topic whose messages are no longer needed.
type NopFlusher struct{}

// NewNopFlusher returns a NopFlusher.
func NewNopFlusher() *NopFlusher {
	return &NopFlusher{}
}

// Flush discards msgs and returns nil.
func (f *NopFlusher) Flush(_ FlushContext, _ []*sqlc.Message) error {
	return nil
}
