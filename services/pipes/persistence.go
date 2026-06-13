package pipes

// Snapshot implements persistence.Persistable by delegating to the backend.
// The versioned snapshot format is defined in backend.go (snapshotV2).
func (h *Handler) Snapshot() []byte {
	return h.Backend.Snapshot()
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(data []byte) error {
	return h.Backend.Restore(data)
}
