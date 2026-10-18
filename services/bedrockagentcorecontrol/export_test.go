package bedrockagentcorecontrol

import "time"

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// ResourceCount returns the stored resource count.
func (b *InMemoryBackend) ResourceCount() int {
	b.mu.RLock("ResourceCount")
	defer b.mu.RUnlock()

	n := 0

	for _, r := range b.resources.All() {
		if b.live(r) {
			n++
		}
	}

	return n
}
