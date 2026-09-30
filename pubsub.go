package pgoutbox

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// PubSubMessage is a single message delivered by a PubSub subscription.
type PubSubMessage struct {
	// Topic is the pub/sub topic the message was published to.
	Topic string `json:"topic"`

	// Payload is the opaque message body. It may be nil: the outbox's own
	// new-message notifications carry no payload, since the notification
	// itself is the signal to check the outbox.
	Payload []byte `json:"payload,omitempty"`
}

// PubSub is a minimal publish/subscribe transport for small notification
// messages. The outbox uses it via WithPubSub to wake Subscribe callers as soon
// as new messages commit, instead of waiting out the poll interval. NewPGPubSub
// provides an implementation built on Postgres LISTEN/NOTIFY, and you can swap
// in your own transport by implementing this interface.
//
// Delivery is best-effort by design. Implementations may drop messages under
// load or while disconnected: the outbox falls back to polling for lost
// messages, and an extra processing pass on an empty topic is a no-op, so
// duplicate or spurious messages are harmless.
type PubSub interface {
	// Pub publishes payload to topic.
	Pub(ctx context.Context, topic string, payload []byte) error

	// Sub subscribes to topic and returns a channel of messages published to
	// it. The subscription lasts until ctx ends (or the PubSub itself shuts
	// down), at which point the channel is closed. The channel should be
	// buffered; implementations may drop messages rather than block when a
	// slow consumer's buffer is full.
	Sub(ctx context.Context, topic string) (<-chan *PubSubMessage, error)
}

// TxPublisher is an optional interface a PubSub can implement to publish
// within a pgx transaction. A notification only makes sense once the inserting
// transaction has committed. When the PubSub passed to WithPubSub implements
// TxPublisher, AddMessages publishes its notification inside the caller's
// transaction, so it is delivered exactly when the insert commits and never for
// a transaction that rolls back. A PubSub that doesn't implement it needs to
// defer notifying until after commit; see WithNotifier.
type TxPublisher interface {
	PubInTx(ctx context.Context, tx pgx.Tx, topic string, payload []byte) error
}
