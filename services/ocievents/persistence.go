package ocievents

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the stored types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Rules   map[string]*Rule    `json:"rules"`
	Auth    []ociauth.KeyRecord `json:"authKeys"`
	Version int                 `json:"version"`
	Seq     int64               `json:"seq"`
}

// Snapshot serialises the rules and registered API keys; the delivery log is not persisted.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocievents", backendSnapshot{
		Version: snapshotVersion, Rules: b.rules, Auth: keys, Seq: b.seq,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocievents", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocievents: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if snap.Rules == nil {
		snap.Rules = map[string]*Rule{}
	}

	for id, r := range snap.Rules {
		if r == nil {
			return fmt.Errorf("%w: rule %q", ErrSnapshotNull, id)
		}
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.rules, b.seq, b.deliveries = snap.Rules, snap.Seq, nil
	b.Auth.Load(snap.Auth)

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ocievents: restore snapshot: %w", err)
	}

	return nil
}
