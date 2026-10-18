package servicequotas

import "time"

// SetClock replaces the time source.
func (b *InMemoryBackend) SetClock(now func() time.Time) {
	b.mu.Lock("SetClock")
	defer b.mu.Unlock()

	b.now = now
}

// SetCatalog replaces the quota catalog.
func (b *InMemoryBackend) SetCatalog(c *Catalog) {
	b.mu.Lock("SetCatalog")
	defer b.mu.Unlock()

	b.catalog = c
}

// RequestCount returns the stored request count.
func (b *InMemoryBackend) RequestCount() int {
	b.mu.RLock("RequestCount")
	defer b.mu.RUnlock()

	return b.requests.Len()
}
