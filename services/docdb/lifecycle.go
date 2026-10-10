package docdb

import "time"

const statusCreating = "creating"

// SetLifecycleDelay sets how long newly created clusters and instances report
// "creating" before "available". Zero (the default) settles instantly.
func (b *InMemoryBackend) SetLifecycleDelay(d time.Duration) {
	b.mu.Lock("SetLifecycleDelay")
	defer b.mu.Unlock()
	b.lifecycleDelay = d
}

func (b *InMemoryBackend) readyAtLocked() time.Time {
	if b.lifecycleDelay <= 0 {
		return time.Time{}
	}

	return time.Now().Add(b.lifecycleDelay)
}

func observedStatus(status string, readyAt time.Time) string {
	if !readyAt.IsZero() && time.Now().Before(readyAt) {
		return statusCreating
	}

	return status
}
