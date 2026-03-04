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

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot() []byte {
	type snapshotter interface{ Snapshot() []byte }
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot()
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	type restorer interface{ Restore([]byte) error }
	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(data)
	}

	return nil
}
