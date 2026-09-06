package azurequeue

import "time"

// Exported wrappers/seams for internal state used in blackbox tests.

// SplitPath exposes splitPath for external tests.
func SplitPath(p string) (string, string, string) {
	return splitPath(p)
}

// SetNowFunc replaces the backend's time provider with fn for deterministic
// testing of visibility-timeout and TTL-expiry logic without real sleeps.
func SetNowFunc(b *InMemoryBackend, fn func() time.Time) {
	b.nowFunc = fn
}

// SetIDFunc replaces the backend's message-ID/pop-receipt generator with fn
// for deterministic testing.
func SetIDFunc(b *InMemoryBackend, fn func() string) {
	b.idFunc = fn
}

// SweepExpired exposes InMemoryBackend.sweepExpired for external tests.
func SweepExpired(b *InMemoryBackend, now time.Time) int {
	return b.sweepExpired(now)
}
