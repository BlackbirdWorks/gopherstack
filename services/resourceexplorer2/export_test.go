package resourceexplorer2

import "time"

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// WriteWaiters reports goroutines blocked on the backend write lock.
func (b *InMemoryBackend) WriteWaiters() int32 {
	_, w, _ := b.mu.GetLockStatus()

	return w
}
