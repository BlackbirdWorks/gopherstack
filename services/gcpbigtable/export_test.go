package gcpbigtable

import "time"

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetDeletedTableCap overrides the bytes of deleted-table data kept per instance.
func (b *InMemoryBackend) SetDeletedTableCap(n int64) {
	b.mu.Lock("SetDeletedTableCap")
	defer b.mu.Unlock()

	b.deletedCap = n
}
