package gcpidentitytoolkit

import (
	"context"
	"fmt"

	idtkv2 "google.golang.org/api/identitytoolkit/v2"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// snapshotVersion must be bumped when a change to the snapshot would make an older one unsafe to decode.
const snapshotVersion = 1

type backendSnapshot struct {
	Projects map[string]*projectRec `json:"projects"`
	Keys     *keyRec                `json:"keys"`
	Version  int                    `json:"version"`
}

// Snapshot serialises the backend state, including the token signing key.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "gcpidentitytoolkit", backendSnapshot{
		Version: snapshotVersion, Projects: b.projects, Keys: b.keys,
	})
}

// Restore loads backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "gcpidentitytoolkit", data, &snap); err != nil {
		return err
	}

	if snap.Version != snapshotVersion {
		logger.Load(ctx).
			WarnContext(ctx, "gcpidentitytoolkit: discarding incompatible snapshot version, starting empty",
				"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		return nil
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	b.projects = map[string]*projectRec{}
	b.keys, b.priv = snap.Keys, nil

	for name, p := range snap.Projects {
		if p == nil || p.Config == nil {
			return fmt.Errorf("%w: project %q", ErrSnapshotNull, name)
		}

		if p.Tenants == nil {
			p.Tenants = map[string]*idtkv2.GoogleCloudIdentitytoolkitAdminV2Tenant{}
		}

		if p.Users == nil {
			p.Users = map[string]map[string]*userRec{}
		}

		if p.Users[rootTenantKey] == nil {
			p.Users[rootTenantKey] = map[string]*userRec{}
		}

		if p.Codes == nil {
			p.Codes = map[string]*oobRec{}
		}

		b.projects[name] = p
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	if err := h.Backend.Restore(ctx, data); err != nil {
		return fmt.Errorf("gcpidentitytoolkit: restore snapshot: %w", err)
	}

	return nil
}
