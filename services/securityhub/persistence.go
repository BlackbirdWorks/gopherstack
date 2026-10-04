package securityhub

import "context"

// Snapshot implements persistence.Persistable; other regions ride in an additive "regions" key.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.peers.Snapshot(snapshotBackend(ctx, h.Backend), func(p *Handler) []byte {
		return snapshotBackend(ctx, p.Backend)
	})
}

func snapshotBackend(ctx context.Context, b StorageBackend) []byte {
	if s, ok := b.(interface{ Snapshot(context.Context) []byte }); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

func restoreBackend(ctx context.Context, b StorageBackend, data []byte) error {
	if r, ok := b.(interface {
		Restore(context.Context, []byte) error
	}); ok {
		return r.Restore(ctx, data)
	}

	return nil
}

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := restoreBackend(ctx, h.Backend, data); err != nil {
		return err
	}

	return h.peers.Restore(
		data,
		func(p *Handler, d []byte) error { return restoreBackend(ctx, p.Backend, d) },
		func(p *Handler) { p.Backend.Reset() },
	)
}
