package azureappconfig

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type backendSnapshot struct {
	Stores  map[string]*store `json:"stores"`
	Version int               `json:"version"`
}

// Snapshot implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	return persistence.MarshalSnapshot(ctx, "azureappconfig", backendSnapshot{
		Version: snapshotVersion, Stores: b.stores,
	})
}

// Restore implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azureappconfig", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "azureappconfig: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)

		b.stores = map[string]*store{}

		return nil
	}

	if err := normalize(&snap); err != nil {
		return err
	}

	b.stores = snap.Stores

	return nil
}

func normalize(snap *backendSnapshot) error {
	if snap.Stores == nil {
		snap.Stores = map[string]*store{}
	}

	for name, s := range snap.Stores {
		if s == nil {
			return fmt.Errorf("%w: store %q", errNullEntry, name)
		}

		if s.KVs == nil {
			s.KVs = map[string]*KeyValue{}
		}

		if s.Snapshots == nil {
			s.Snapshots = map[string]*Snapshot{}
		}

		for id, kv := range s.KVs {
			if kv == nil {
				return fmt.Errorf("%w: key-value %q", errNullEntry, id)
			}
		}

		for sn, snapshot := range s.Snapshots {
			if snapshot == nil {
				return fmt.Errorf("%w: snapshot %q", errNullEntry, sn)
			}
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
