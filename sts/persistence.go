package sts

// Snapshot serialises the backend state to JSON.
// STS has no mutable in-memory data to persist, so it always returns nil.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot() []byte {
	return nil
}

// Restore loads backend state from a JSON snapshot.
// STS has no mutable in-memory data to restore, so it is a no-op.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(_ []byte) error {
	return nil
}
