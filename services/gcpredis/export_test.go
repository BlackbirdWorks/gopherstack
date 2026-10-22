package gcpredis

import "time"

// SetClock replaces the backend clock.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetJobGate makes every background operation call f before it starts working.
func (b *InMemoryBackend) SetJobGate(f func()) { b.gate = f }
