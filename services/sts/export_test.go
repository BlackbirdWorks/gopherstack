package sts

import "time"

// SessionCount returns the number of sessions currently stored in the backend.
// Used in tests to verify janitor eviction.
func (b *InMemoryBackend) SessionCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.sessions)
}

// SetSessionExpiration overrides the expiration of the session identified by
// accessKeyID. Used in tests to fast-forward session expiry without waiting.
func (b *InMemoryBackend) SetSessionExpiration(accessKeyID string, exp time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if s, ok := b.sessions[accessKeyID]; ok {
		s.Expiration = exp
	}
}
