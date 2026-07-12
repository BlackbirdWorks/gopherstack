package quicksight

// Code in this file wires up snapshot/restore persistence for the QuickSight
// service at the Handler level. InMemoryBackend.Snapshot/Restore (backend.go)
// already implement a full round trip over every registered resource
// collection (see the registry-backed Snapshot/Restore above) -- but until
// now nothing on *Handler exposed that: cli.go's setupPersistence registers a
// service.Registerable in the persistence.Manager only if the
// service.Registerable itself (the *Handler returned by Provider.Init, not
// the backend it wraps) satisfies Snapshot(ctx) []byte / Restore(ctx,
// []byte) error. Handler.Backend is the StorageBackend interface, so those
// backend methods are not promoted automatically; the two delegating methods
// below close that gap. Mirrors services/securityhub's identical
// Handler-level delegation.
import "context"

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}

	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}

	if r, ok := h.Backend.(restorer); ok {
		return r.Restore(ctx, data)
	}

	return nil
}
