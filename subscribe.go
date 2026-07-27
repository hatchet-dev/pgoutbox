package pgoutbox

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// defaultPollInterval is how often Subscribe re-checks the topic when the
// caller has not specified WithPollInterval. With a PubSub configured the
// poll is only a safety net for lost notifications, so it can be generous.
const defaultPollInterval = 5 * time.Second

// SubscribeOpt is a per-call option for Subscribe.
type SubscribeOpt func(*subscribeOpts)

type subscribeOpts struct {
	pollInterval time.Duration
	processOpts  []ProcessOpt
	exclusive    bool
}

func defaultSubscribeOpts() *subscribeOpts {
	return &subscribeOpts{pollInterval: defaultPollInterval}
}

// WithPollInterval sets how long Subscribe waits between processing passes
// when no new-message notification arrives. Must be > 0.
func WithPollInterval(d time.Duration) SubscribeOpt {
	return func(opts *subscribeOpts) {
		if d <= 0 {
			return
		}
		opts.pollInterval = d
	}
}

// WithProcessOpts forwards per-call ProcessMessages options (e.g.
// WithBatchSize) to every processing pass Subscribe makes.
func WithProcessOpts(popts ...ProcessOpt) SubscribeOpt {
	return func(opts *subscribeOpts) {
		opts.processOpts = append(opts.processOpts, popts...)
	}
}

// WithExclusive makes Subscribe manage the topic's exclusive-consumer lease
// for the duration of the call: it acquires the lease before the first
// processing pass (blocking, like AcquireTopic, while another instance holds
// it), re-acquires it if it is ever lost mid-subscribe, and releases it on
// return so a waiting instance can take over immediately instead of waiting
// out the lease's grace period. Several instances calling Subscribe with
// WithExclusive on the same topic therefore form a failover group: exactly
// one drains the topic while the rest block in line behind the lease.
func WithExclusive() SubscribeOpt {
	return func(opts *subscribeOpts) {
		opts.exclusive = true
	}
}

func (o *outboxImpl) Subscribe(ctx context.Context, topic string, sopts ...SubscribeOpt) error {
	if topic == "" {
		return fmt.Errorf("topic must not be empty")
	}
	if _, ok := o.getFlusher(topic); !ok {
		return fmt.Errorf("no flusher registered for topic %q", topic)
	}

	so := defaultSubscribeOpts()
	for _, opt := range sopts {
		opt(so)
	}

	if so.exclusive {
		if err := o.AcquireTopic(ctx, topic); err != nil {
			return fmt.Errorf("could not acquire exclusive lease for topic %q: %w", topic, err)
		}
		// Hand the lease off immediately on exit instead of letting it lapse
		// through the grace period. Detached from ctx (the usual way out is
		// this ctx being cancelled) but bounded, so a stalled database can't
		// hang subscriber shutdown; a missed release degrades to the
		// grace-period lapse inside ReleaseTopic itself.
		defer func() {
			releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), exclusiveLeaseWriteTimeout)
			defer cancel()
			if err := o.ReleaseTopic(releaseCtx, topic); err != nil {
				o.logger.Error().Err(err).Str("topic", topic).Msg("subscribe: failed to release exclusive lease")
			}
		}()
	}

	var notifications <-chan *PubSubMessage
	if o.pubsub != nil {
		ch, err := o.pubsub.Sub(ctx, topic)
		if err != nil {
			return fmt.Errorf("could not subscribe to notifications for topic %q: %w", topic, err)
		}
		notifications = ch
	}

	pollTimer := time.NewTimer(so.pollInterval)
	defer pollTimer.Stop()

	for {
		// Anything already buffered is covered by the pass below — drop it now
		// so it doesn't immediately trigger a redundant pass. A notification
		// arriving after this point stays in the buffer and wakes the select,
		// which is correct: its messages may have committed after our pass.
		drainNotifications(notifications)

		err := o.processTopicUntilEmpty(ctx, topic, so.processOpts)

		if so.exclusive && isExclusiveLeaseError(err) {
			// The lease was lost mid-subscribe (session lapse, takeover after
			// a grace expiry). Block until we hold it again, then drain right
			// away: notifications that arrived meanwhile coalesced in the
			// buffer, and the post-acquire pass covers the backlog regardless.
			if acqErr := o.AcquireTopic(ctx, topic); acqErr != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				// Transient failure: fall through to the select and let the
				// next wake-up retry the pass (and this re-acquisition).
				o.logger.Error().Err(acqErr).Str("topic", topic).Msg("subscribe: failed to re-acquire exclusive lease")
			} else {
				continue
			}
		}

		// One reused timer instead of a time.After per iteration, so frequent
		// notification wake-ups don't allocate a timer each loop. Reset
		// discards any pending fire from a previous iteration (Go 1.23+ timer
		// semantics), giving each wait the full poll interval.
		pollTimer.Reset(so.pollInterval)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pollTimer.C:
		case _, ok := <-notifications:
			if !ok {
				// The PubSub shut down before our ctx did; degrade to polling.
				notifications = nil
			}
		}
	}
}

// isExclusiveLeaseError reports whether a ProcessMessages failure means this
// instance does not (or no longer) hold the topic's exclusive lease.
func isExclusiveLeaseError(err error) bool {
	return errors.Is(err, ErrExclusiveLeaseHeld) || errors.Is(err, ErrExclusiveLeaseRequired)
}

// processTopicUntilEmpty runs ProcessMessages until the topic comes back
// empty, returning the error that ended the pass (nil when the topic was
// drained). Errors are logged here and implicitly retried when the next poll
// tick or notification wakes the Subscribe loop; the return value exists so
// Subscribe can recognize lease loss and re-acquire.
func (o *outboxImpl) processTopicUntilEmpty(ctx context.Context, topic string, popts []ProcessOpt) error {
	for ctx.Err() == nil {
		msgs, err := o.ProcessMessages(ctx, topic, popts...)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				o.logger.Error().Err(err).Str("topic", topic).Msg("subscribe: failed to process messages")
			}
			return err
		}
		if len(msgs) == 0 {
			return nil
		}
	}
	return ctx.Err()
}

// drainNotifications empties ch without blocking. A nil or closed channel is
// a no-op.
func drainNotifications(ch <-chan *PubSubMessage) {
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		default:
			return
		}
	}
}
