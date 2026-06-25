package pgoutbox

import "time"

// SetMaintenanceLeaseTimeoutForTest overrides maintenanceLeaseTimeout for the
// duration of a test and returns a restore function. Only compiled during tests.
func SetMaintenanceLeaseTimeoutForTest(d time.Duration) func() {
	old := maintenanceLeaseTimeout
	maintenanceLeaseTimeout = d
	return func() { maintenanceLeaseTimeout = old }
}

// SetTopicScanIntervalForTest overrides topicScanInterval for the duration of
// a test and returns a restore function. Only compiled during tests.
func SetTopicScanIntervalForTest(d time.Duration) func() {
	old := topicScanInterval
	topicScanInterval = d
	return func() { topicScanInterval = old }
}

func SetExclusiveLeaseDurationForTest(d time.Duration) func() {
	old := exclusiveLeaseDuration
	exclusiveLeaseDuration = d
	return func() { exclusiveLeaseDuration = old }
}

func SetExclusiveLeaseRenewIntervalForTest(d time.Duration) func() {
	old := exclusiveLeaseRenewInterval
	exclusiveLeaseRenewInterval = d
	return func() { exclusiveLeaseRenewInterval = old }
}

func SetExclusiveLeaseRetryIntervalForTest(d time.Duration) func() {
	old := exclusiveLeaseRetryInterval
	exclusiveLeaseRetryInterval = d
	return func() { exclusiveLeaseRetryInterval = old }
}
