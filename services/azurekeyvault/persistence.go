package azurekeyvault

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a stored type changes incompatibly.
const snapshotVersion = 1

type backendSnapshot struct {
	Vaults  map[string]*vault `json:"vaults"`
	Version int               `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(
		ctx,
		"azurekeyvault",
		backendSnapshot{Version: snapshotVersion, Vaults: b.vaults},
	)
}

// Restore loads backend state from a JSON snapshot. It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azurekeyvault", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azurekeyvault: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.vaults = make(map[string]*vault)

		return nil
	}

	vaults := make(map[string]*vault, len(snap.Vaults))

	for key, v := range snap.Vaults {
		if v == nil {
			return fmt.Errorf("%w: vault %q", ErrSnapshotNull, key)
		}

		v.normalize()
		vaults[key] = v
	}

	b.vaults = vaults

	return nil
}

func (v *vault) normalize() {
	if v.Secrets == nil {
		v.Secrets = make(Collection[*SecretVersion])
	}

	if v.Keys == nil {
		v.Keys = make(Collection[*KeyVersion])
	}

	if v.Certs == nil {
		v.Certs = make(Collection[*CertVersion])
	}

	if v.Issuers == nil {
		v.Issuers = make(map[string]*Issuer)
	}

	if v.Pending == nil {
		v.Pending = make(map[string]*PendingCert)
	}

	for _, e := range v.Keys {
		if e.Rotation == nil {
			e.Rotation = defaultRotation(0)
		}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("azurekeyvault: restore snapshot: %w", err)
	}

	return nil
}
