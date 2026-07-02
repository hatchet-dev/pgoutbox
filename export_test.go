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
