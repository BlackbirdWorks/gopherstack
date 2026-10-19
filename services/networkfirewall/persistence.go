package networkfirewall

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const networkFirewallSnapshotVersion = 1

// backendSnapshot is the on-disk shape: one JSON array per registered table.
type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`
}

// Snapshot serializes backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "networkfirewall: snapshot table marshal failed", "error", err)

		return nil
	}

	return persistence.MarshalSnapshot(ctx, "networkfirewall", &backendSnapshot{
		Version: networkFirewallSnapshotVersion,
		Tables:  tables,
	})
}

// Restore deserializes backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "networkfirewall", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != networkFirewallSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"networkfirewall: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", networkFirewallSnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("networkfirewall: restore snapshot tables: %w", err)
	}

	return nil
}
