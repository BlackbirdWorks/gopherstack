package azurecontainerregistry

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/ociregistry"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/strs"
)

// snapshotVersion must be bumped when a change to the snapshot types would make an older snapshot unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Registries map[string]*registry `json:"registries"`
	Version    int                  `json:"version"`
}

// Snapshot serialises registries, credentials and image metadata. Blobs and manifests are not part of it:
// they live under the data directory when one is configured.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "azurecontainerregistry", backendSnapshot{
		Version: snapshotVersion, Registries: b.regs,
	})
}

// Restore loads a snapshot, replacing every registry.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azurecontainerregistry", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).
			WarnContext(ctx, "azurecontainerregistry: discarding incompatible snapshot version, starting empty",
				"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.replace(map[string]*registry{})

		return nil
	}

	regs, err := normalize(snap.Registries)
	if err != nil {
		return err
	}

	b.replace(regs)

	return nil
}

func (b *InMemoryBackend) replace(regs map[string]*registry) {
	for _, r := range b.regs {
		b.release(r, false)
	}

	b.regs = regs
}

func normalize(in map[string]*registry) (map[string]*registry, error) {
	out := make(map[string]*registry, len(in))

	for _, r := range in {
		if r == nil {
			return nil, fmt.Errorf("%w: registry", errNullEntry)
		}

		if r.Repos == nil {
			r.Repos = ociregistry.Ledger{}
		}

		if err := r.Repos.Normalize(); err != nil {
			return nil, err
		}

		out[strs.Fold(r.Name)] = r
	}

	return out, nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("azurecontainerregistry: restore snapshot: %w", err)
	}

	return nil
}
