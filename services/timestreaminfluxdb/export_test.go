package timestreaminfluxdb

import "time"

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// InstanceCount returns the stored instances, deleting ones included.
func (b *InMemoryBackend) InstanceCount() int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return b.instances.Len()
}
