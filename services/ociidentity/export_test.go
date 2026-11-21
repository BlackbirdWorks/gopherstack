package ociidentity

import "sync/atomic"

// ReplayHeld counts the plaintext secrets kept in memory for retry-token replay.
func (b *InMemoryBackend) ReplayHeld() int {
	b.mu.RLock("ReplayHeld")
	defer b.mu.RUnlock()

	return len(b.authReplay)
}

// CountDerivations wraps the key derivation and returns a counter of its calls.
func (b *InMemoryBackend) CountDerivations() *atomic.Int64 {
	n := &atomic.Int64{}
	inner := b.derive

	b.derive = func(secret string, salt []byte) []byte {
		n.Add(1)

		return inner(secret, salt)
	}

	return n
}
