package pgoutbox

import "time"

// SetMaintenanceLeaseTimeoutForTest overrides maintenanceLeaseTimeout for the
// duration of a test and returns a restore function. Only compiled during tests.
func SetMaintenanceLeaseTimeoutForTest(d time.Duration) func() {
	old := maintenanceLeaseTimeout.Load()
	maintenanceLeaseTimeout.Store(d)
	return func() { maintenanceLeaseTimeout.Store(old) }
}

// SetTopicScanIntervalForTest overrides topicScanInterval for the duration of
// a test and returns a restore function. Only compiled during tests.
func SetTopicScanIntervalForTest(d time.Duration) func() {
	old := topicScanInterval.Load()
	topicScanInterval.Store(d)
	return func() { topicScanInterval.Store(old) }
}

func SetExclusiveLeaseDurationForTest(d time.Duration) func() {
	old := exclusiveLeaseDuration.Load()
	exclusiveLeaseDuration.Store(d)
	return func() { exclusiveLeaseDuration.Store(old) }
}

func SetExclusiveLeaseRenewIntervalForTest(d time.Duration) func() {
	old := exclusiveLeaseRenewInterval.Load()
	exclusiveLeaseRenewInterval.Store(d)
	return func() { exclusiveLeaseRenewInterval.Store(old) }
}

func SetExclusiveLeaseRetryIntervalForTest(d time.Duration) func() {
	old := exclusiveLeaseRetryInterval.Load()
	exclusiveLeaseRetryInterval.Store(d)
	return func() { exclusiveLeaseRetryInterval.Store(old) }
}

func SetMaintenanceMinIntervalForTest(d time.Duration) func() {
	old := maintenanceMinInterval.Load()
	maintenanceMinInterval.Store(d)
	return func() { maintenanceMinInterval.Store(old) }
}

func SetMaintenanceActivitySlackForTest(d time.Duration) func() {
	old := maintenanceActivitySlack.Load()
	maintenanceActivitySlack.Store(d)
	return func() { maintenanceActivitySlack.Store(old) }
}

func SetMaintenanceCatchupIntervalForTest(d time.Duration) func() {
	old := maintenanceCatchupInterval.Load()
	maintenanceCatchupInterval.Store(d)
	return func() { maintenanceCatchupInterval.Store(old) }
}

// ManagedTopicsForTest returns the topics whose maintenance loops o currently
// tracks. Only compiled during tests.
func ManagedTopicsForTest(o Outbox) []string {
	impl := o.(*outboxImpl)
	impl.managedMu.Lock()
	defer impl.managedMu.Unlock()
	topics := make([]string, 0, len(impl.managed))
	for topic := range impl.managed {
		topics = append(topics, topic)
	}
	return topics
}
