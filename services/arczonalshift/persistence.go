package arczonalshift

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`
}

// Snapshot serialises the backend state. It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "arczonalshift: snapshot table marshal failed", "error", err)

		return nil
	}

	return persistence.MarshalSnapshot(ctx, "arczonalshift", backendSnapshot{Version: snapshotVersion, Tables: tables})
}

// Restore loads a snapshot; an incompatible version starts empty. It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "arczonalshift", data, &snap); err != nil {
		return err
	}

	if b.mu == nil {
		b.mu = lockmetrics.New("arczonalshift")
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("arczonalshift: restore snapshot tables: %w", err)
	}

	return nil
}
