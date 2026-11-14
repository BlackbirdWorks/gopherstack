package dsql

import "time"

// SetClock replaces the wall clock used for lifecycle deadlines; nil restores time.Now.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.clock = now
}

// now returns the current time; callers hold b.mu.
func (b *InMemoryBackend) now() time.Time {
	if b.clock != nil {
		return b.clock()
	}

	return time.Now()
}
