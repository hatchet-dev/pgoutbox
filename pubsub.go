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
// messages. The outbox uses it (via WithPubSub) to wake Subscribe callers as
// soon as new messages are staged, instead of waiting out a poll interval.
//
// Delivery is expected to be best-effort: implementations may drop messages
// under load or while disconnected. The outbox tolerates both lost messages
// (Subscribe falls back to polling) and duplicate or spurious messages (an
// extra processing pass on an empty topic is a no-op).
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
// within a pgx transaction. When the PubSub configured via WithPubSub
// implements it (detected once, at NewOutbox), AddMessages publishes its
// new-message notification inside the caller's transaction, so the
// notification is delivered exactly when the insert commits — and never for a
// transaction that rolls back. Without it, the notification is deferred to a
// Notifier the caller passes via WithNotifier and invokes after commit.
type TxPublisher interface {
	PubInTx(ctx context.Context, tx pgx.Tx, topic string, payload []byte) error
}
