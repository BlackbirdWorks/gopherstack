package rum

import "time"

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// MonitorCount returns the stored app monitor count.
func (b *InMemoryBackend) MonitorCount() int {
	b.mu.RLock("MonitorCount")
	defer b.mu.RUnlock()

	return b.monitors.Len()
}

// EventCount returns the stored event count.
func (b *InMemoryBackend) EventCount() int {
	b.mu.RLock("EventCount")
	defer b.mu.RUnlock()

	return b.events.Len()
}

// MatchPattern exposes the event-pattern matcher.
func MatchPattern(pattern, doc map[string]any) bool { return matchPattern(pattern, doc) }
