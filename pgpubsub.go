package pgoutbox

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// defaultNotifyChannel is the Postgres NOTIFY channel all pgPubSub messages
// are multiplexed over. Every message carries its pub/sub topic in the JSON
// envelope, so one LISTEN connection serves any number of topics.
const defaultNotifyChannel = "pgoutbox_pubsub"

// subscriberBufferSize is the capacity of each subscription channel. When a
// subscriber falls this far behind, further messages are dropped rather than
// blocking the listener — acceptable because delivery is best-effort and the
// outbox's notifications are pure wake-up signals that coalesce naturally.
const subscriberBufferSize = 16

// pgNotifyReconnectDelay is how long the listener waits before redialing
// after its connection fails. Exposed as a var for tests.
var pgNotifyReconnectDelay = newAtomicDuration(5 * time.Second)

type pgPubSubOpts struct {
	channel string
	logger  zerolog.Logger
}

func defaultPGPubSubOpts() *pgPubSubOpts {
	return &pgPubSubOpts{
		channel: defaultNotifyChannel,
		logger:  zerolog.Nop(),
	}
}

// PGPubSubOpt configures the PubSub returned by NewPGPubSub.
type PGPubSubOpt func(*pgPubSubOpts)

// WithNotifyChannel overrides the Postgres NOTIFY channel the PubSub
// multiplexes over. All messages on a channel are broadcast to every listener
// of that channel, so two outboxes sharing a database (e.g. different
// schemas) should use distinct channels to avoid spurious wake-ups.
func WithNotifyChannel(name string) PGPubSubOpt {
	return func(opts *pgPubSubOpts) {
		opts.channel = name
	}
}

// WithNotifyLogger attaches a zerolog logger that receives errors from the
// background listener (connection failures, malformed payloads). If not set,
// those errors are silent.
func WithNotifyLogger(l zerolog.Logger) PGPubSubOpt {
	return func(opts *pgPubSubOpts) {
		opts.logger = l
	}
}

// pgPubSub implements PubSub (and TxPublisher) on top of Postgres
// LISTEN/NOTIFY. All messages travel over a single NOTIFY channel wrapped in
// a JSON envelope carrying the pub/sub topic; a lone background listener
// dispatches them to in-process subscribers.
//
// The listener runs on a connection hijacked out of the pool: it holds LISTEN
// session state and blocks in WaitForNotification indefinitely, so it must
// neither occupy a pool slot nor ever be handed back for reuse.
type pgPubSub struct {
	pool    *pgxpool.Pool
	channel string
	logger  zerolog.Logger

	// ctx bounds the background listener; it is the ctx passed to NewPGPubSub.
	ctx context.Context

	// listenMu guards listening, the lazy one-time start of the listener
	// goroutine. Held across the initial dial so that Sub only returns once
	// LISTEN is active — messages published after a successful Sub are
	// delivered (barring connection loss) rather than racing the setup.
	listenMu  sync.Mutex
	listening bool

	subscribersMu sync.Mutex
	subscribers   map[string]map[chan *PubSubMessage]struct{}
}

// NewPGPubSub returns a PubSub backed by Postgres LISTEN/NOTIFY on the given
// pool. The background listener starts lazily on the first Sub call and runs
// until ctx is cancelled; pass a context tied to your application lifetime.
//
// The returned PubSub implements TxPublisher, so an outbox configured with it
// publishes new-message notifications transactionally: subscribers wake when
// the staging transaction commits, and not at all if it rolls back.
//
// NOTIFY payloads are capped by Postgres at roughly 8000 bytes; Pub returns
// an error beyond that. The outbox's own notifications are empty.
func NewPGPubSub(ctx context.Context, pool *pgxpool.Pool, fs ...PGPubSubOpt) (PubSub, error) {
	opts := defaultPGPubSubOpts()

	for _, f := range fs {
		f(opts)
	}

	// The channel name is interpolated into the LISTEN statement, so restrict
	// it to identifier-safe characters just like the schema name.
	if !schemaNameRE.MatchString(opts.channel) {
		return nil, fmt.Errorf("invalid notify channel name %q: must match %s", opts.channel, schemaNameRE)
	}

	return &pgPubSub{
		pool:        pool,
		channel:     opts.channel,
		logger:      opts.logger,
		ctx:         ctx,
		subscribers: make(map[string]map[chan *PubSubMessage]struct{}),
	}, nil
}

func marshalPubSubMessage(topic string, payload []byte) ([]byte, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic must not be empty")
	}
	wrapped, err := json.Marshal(&PubSubMessage{Topic: topic, Payload: payload})
	if err != nil {
		return nil, fmt.Errorf("could not marshal pubsub message for topic %q: %w", topic, err)
	}
	return wrapped, nil
}

func (p *pgPubSub) Pub(ctx context.Context, topic string, payload []byte) error {
	wrapped, err := marshalPubSubMessage(topic, payload)
	if err != nil {
		return err
	}
	if _, err := p.pool.Exec(ctx, "select pg_notify($1, $2)", p.channel, string(wrapped)); err != nil {
		return fmt.Errorf("could not publish to topic %q: %w", topic, err)
	}
	return nil
}

// PubInTx publishes on the caller's transaction, deferring delivery to commit
// time. Postgres deduplicates identical notifications within a transaction,
// so repeated AddMessages calls for one topic in one transaction cost a
// single wake-up.
func (p *pgPubSub) PubInTx(ctx context.Context, tx pgx.Tx, topic string, payload []byte) error {
	wrapped, err := marshalPubSubMessage(topic, payload)
	if err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, "select pg_notify($1, $2)", p.channel, string(wrapped)); err != nil {
		return fmt.Errorf("could not publish to topic %q: %w", topic, err)
	}
	return nil
}

func (p *pgPubSub) Sub(ctx context.Context, topic string) (<-chan *PubSubMessage, error) {
	if topic == "" {
		return nil, fmt.Errorf("topic must not be empty")
	}
	if err := p.ctx.Err(); err != nil {
		return nil, fmt.Errorf("pubsub has shut down: %w", err)
	}

	if err := p.ensureListening(ctx); err != nil {
		return nil, err
	}

	ch := make(chan *PubSubMessage, subscriberBufferSize)

	p.subscribersMu.Lock()
	subs := p.subscribers[topic]
	if subs == nil {
		subs = make(map[chan *PubSubMessage]struct{})
		p.subscribers[topic] = subs
	}
	subs[ch] = struct{}{}
	p.subscribersMu.Unlock()

	go func() {
		select {
		case <-ctx.Done():
		case <-p.ctx.Done():
		}
		p.unsubscribe(topic, ch)
	}()

	return ch, nil
}

func (p *pgPubSub) unsubscribe(topic string, ch chan *PubSubMessage) {
	p.subscribersMu.Lock()
	defer p.subscribersMu.Unlock()

	subs := p.subscribers[topic]
	if _, ok := subs[ch]; !ok {
		return
	}
	delete(subs, ch)
	if len(subs) == 0 {
		delete(p.subscribers, topic)
	}
	// Closing under subscribersMu is what makes it safe: dispatch sends while
	// holding the same lock, so a send on a closed channel is impossible.
	close(ch)
}

func (p *pgPubSub) dispatch(msg *PubSubMessage) {
	p.subscribersMu.Lock()
	defer p.subscribersMu.Unlock()

	for ch := range p.subscribers[msg.Topic] {
		// Every subscriber gets its own copy, payload included, so a consumer
		// that mutates a delivered message can't affect (or race) the others.
		delivery := &PubSubMessage{Topic: msg.Topic, Payload: slices.Clone(msg.Payload)}
		select {
		case ch <- delivery:
		default:
			// Subscriber buffer full — drop rather than block the listener.
		}
	}
}

// ensureListening lazily starts the single background listener. The initial
// dial happens synchronously so a successful Sub means LISTEN is already
// active; reconnects after that are handled in the background.
func (p *pgPubSub) ensureListening(ctx context.Context) error {
	p.listenMu.Lock()
	defer p.listenMu.Unlock()

	if p.listening {
		return nil
	}

	conn, err := p.connectAndListen(ctx)
	if err != nil {
		return fmt.Errorf("could not start pubsub listener: %w", err)
	}

	p.listening = true
	go p.runListener(conn)
	return nil
}

// connectAndListen acquires a connection, hijacks it out of the pool, and
// issues LISTEN on it. On success the caller owns the connection and is
// responsible for closing it.
func (p *pgPubSub) connectAndListen(ctx context.Context) (*pgx.Conn, error) {
	poolConn, err := p.pool.Acquire(ctx)
	if err != nil {
		return nil, fmt.Errorf("acquire listener connection: %w", err)
	}

	conn := poolConn.Hijack()

	if _, err := conn.Exec(ctx, "listen "+pgx.Identifier{p.channel}.Sanitize()); err != nil {
		_ = conn.Close(context.WithoutCancel(ctx))
		return nil, fmt.Errorf("listen on channel %q: %w", p.channel, err)
	}

	return conn, nil
}

// runListener serves notifications off conn until p.ctx ends, redialing with
// a delay whenever the connection fails. Notifications sent while
// disconnected are lost — subscribers are expected to treat delivery as
// best-effort (Subscribe covers gaps with its poll interval).
func (p *pgPubSub) runListener(conn *pgx.Conn) {
	for {
		err := p.serveConn(conn)
		_ = conn.Close(context.WithoutCancel(p.ctx))

		if p.ctx.Err() != nil {
			return
		}
		p.logger.Error().Err(err).Msg("pubsub: listener connection failed; reconnecting")

		for {
			select {
			case <-p.ctx.Done():
				return
			case <-time.After(pgNotifyReconnectDelay.Load()):
			}

			next, err := p.connectAndListen(p.ctx)
			if err != nil {
				if p.ctx.Err() != nil {
					return
				}
				p.logger.Error().Err(err).Msg("pubsub: failed to reconnect listener")
				continue
			}
			conn = next
			break
		}
	}
}

func (p *pgPubSub) serveConn(conn *pgx.Conn) error {
	for {
		notification, err := conn.WaitForNotification(p.ctx)
		if err != nil {
			return err
		}

		msg := &PubSubMessage{}
		if err := json.Unmarshal([]byte(notification.Payload), msg); err != nil {
			p.logger.Error().Err(err).Msg("pubsub: dropping notification with malformed payload")
			continue
		}

		p.dispatch(msg)
	}
}
