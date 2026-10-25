package ocivault

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociauth"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to Vault/Key/Secret would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Vaults  map[string]*Vault   `json:"vaults"`
	Keys    map[string]*Key     `json:"keys"`
	Secrets map[string]*Secret  `json:"secrets"`
	Auth    []ociauth.KeyRecord `json:"authKeys"`
	Version int                 `json:"version"`
	Seq     int64               `json:"seq"`
}

// Snapshot serialises the backend state, including registered API keys.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	keys := b.Auth.Records()

	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "ocivault", backendSnapshot{
		Version: snapshotVersion, Vaults: b.vaults, Keys: b.keys, Secrets: b.secrets, Auth: keys, Seq: b.seq,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ocivault", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "ocivault: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.Reset()

		return nil
	}

	if err := checkSnapshot(&snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.vaults, b.keys, b.secrets, b.seq = snap.Vaults, snap.Keys, snap.Secrets, snap.Seq
	b.Auth.Load(snap.Auth)

	return nil
}

func checkSnapshot(snap *backendSnapshot) error {
	if snap.Vaults == nil {
		snap.Vaults = map[string]*Vault{}
	}

	if snap.Keys == nil {
		snap.Keys = map[string]*Key{}
	}

	if snap.Secrets == nil {
		snap.Secrets = map[string]*Secret{}
	}

	for id, v := range snap.Vaults {
		if v == nil {
			return fmt.Errorf("%w: vault %q", ErrSnapshotNull, id)
		}
	}

	for id, k := range snap.Keys {
		if k == nil {
			return fmt.Errorf("%w: key %q", ErrSnapshotNull, id)
		}
	}

	for id, s := range snap.Secrets {
		if s == nil {
			return fmt.Errorf("%w: secret %q", ErrSnapshotNull, id)
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("ocivault: restore snapshot: %w", err)
	}

	return nil
}
