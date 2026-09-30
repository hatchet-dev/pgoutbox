// Package pgoutbox implements a simple transactional outbox for pgx.
//
// New messages are added to a Postgres table within a transaction using
// [Outbox.AddMessages] and are flushed to a destination via
// [Outbox.ProcessMessages]. Because the insert rides the caller's own
// transaction, a message is only ever visible to consumers if the business
// write it belongs to committed.
//
// Messages are grouped by topic, and each topic is drained by the [Flusher]
// registered for it with [Outbox.AddFlusher]. ProcessMessages locks a batch of
// messages, hands the whole batch to a single Flush call, and deletes the
// messages in the same transaction if the flush succeeds. A Flusher that
// writes to Postgres itself can use [FlushContext.Tx] so its writes and the
// outbox delete commit or roll back together.
//
// [Outbox.Subscribe] drains a topic continuously, waking on a poll interval
// or, when a [PubSub] such as [NewPGPubSub] is attached via [WithPubSub], the
// moment new messages commit. [Outbox.AcquireTopic] and [WithExclusive] give
// a topic exactly one active consumer across a fleet, backed by a renewing
// lease with automatic failover. [WithTopicExpiration] and
// [WithDefaultExpiration] delete old messages in the background.
//
// A minimal setup looks like:
//
//	type printFlusher struct{}
//
//	func (printFlusher) Flush(_ pgoutbox.FlushContext, msgs []*sqlc.Message) error {
//		for _, m := range msgs {
//			fmt.Printf("flushed id=%d topic=%s payload=%s\n", m.ID, m.Topic, string(m.Payload))
//		}
//		return nil
//	}
//
//	outbox, err := pgoutbox.NewOutbox(ctx, pool)
//	if err != nil {
//		return err
//	}
//	outbox.AddFlusher("orders", printFlusher{})
//
//	// within a transaction
//	err = outbox.AddMessages(ctx, tx, "orders", []pgoutbox.MessageOpts{{Payload: payload}})
//
//	// after the transaction commits
//	_, err = outbox.ProcessMessages(ctx, "orders")
//
// See the README at https://github.com/hatchet-dev/pgoutbox for a longer
// walkthrough of each feature.
package pgoutbox
