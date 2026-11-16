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

// RecordSynchronization returns the persisted synchronization request of a registry record.
func (b *InMemoryBackend) RecordSynchronization(recordID string) (map[string]any, bool) {
	b.mu.RLock("RecordSynchronization")
	defer b.mu.RUnlock()

	for _, r := range b.resources.All() {
		if r.Kind == kindRegistryRecord && r.ID == recordID {
			s, ok := r.Aux[keySyncRecord].(map[string]any)

			return s, ok
		}
	}

	return nil, false
}

// AuthSessionCount returns the number of stored OAuth authorization sessions.
func (b *InMemoryBackend) AuthSessionCount() int {
	b.mu.RLock("AuthSessionCount")
	defer b.mu.RUnlock()

	return len(b.sessions.All())
}

// ErrSessionMismatch exposes the user-mismatch error for tests.
var ErrSessionMismatch = errSessionMismatch

// JWKSCacheSize returns the number of cached issuer key sets.
func (b *InMemoryBackend) JWKSCacheSize() int {
	b.mu.RLock("JWKSCacheSize")
	defer b.mu.RUnlock()

	return len(b.jwksCache)
}
