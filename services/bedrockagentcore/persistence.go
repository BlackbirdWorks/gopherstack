package bedrockagentcore

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

const snapshotVersion = 1

type backendSnapshot struct {
	Tables  map[string]json.RawMessage `json:"tables"`
	Version int                        `json:"version"`
}

// Snapshot serialises the backend state. Containers and sandboxes are not part of it.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "bedrockagentcore: snapshot marshal failed", "error", err)

		return nil
	}

	return persistence.MarshalSnapshot(ctx, serviceName, &backendSnapshot{Version: snapshotVersion, Tables: tables})
}

// Restore loads a snapshot; an incompatible version is discarded. Sessions that were READY come back TERMINATED
// because their sandboxes did not survive the restart.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, serviceName, data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != snapshotVersion {
		logger.Load(ctx).WarnContext(ctx, "bedrockagentcore: discarding incompatible snapshot",
			"gotVersion", snap.Version, "wantVersion", snapshotVersion)
		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("bedrockagentcore: restore snapshot tables: %w", err)
	}

	b.afterRestoreLocked()

	return nil
}

func (b *InMemoryBackend) afterRestoreLocked() {
	b.seq = 0

	for _, e := range b.events.All() {
		b.seq = max(b.seq, e.Seq)
	}

	for _, r := range b.records.All() {
		b.seq = max(b.seq, r.Seq)
	}

	for _, s := range b.sessions.All() {
		b.terminateLocked(s)
	}
}

// Snapshot implements persistence.Persistable.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
