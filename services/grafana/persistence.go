package grafana

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// grafanaSnapshotVersion identifies the shape of backendSnapshot's Tables
// blob (the set of resources registered on b.registry -- see
// registerAllTables in store_setup.go). Bump whenever a change there would
// make an older snapshot unsafe to decode as the current shape; Restore
// discards (rather than partially decodes) any mismatch -- see Restore.
const grafanaSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Grafana backend.
type backendSnapshot struct {
	Tables               map[string]json.RawMessage `json:"tables"`
	AccountID            string                     `json:"accountId"`
	Region               string                     `json:"region"`
	Version              int                        `json:"version"`
	NextServiceAccountID uint64                     `json:"nextServiceAccountId"`
	NextTokenID          uint64                     `json:"nextTokenId"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "grafana: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:              grafanaSnapshotVersion,
		Tables:               tables,
		AccountID:            b.accountID,
		Region:               b.region,
		NextServiceAccountID: b.nextServiceAccountID,
		NextTokenID:          b.nextTokenID,
	}

	return persistence.MarshalSnapshot(ctx, "grafana", snap)
}

// Restore loads backend state from a JSON snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "grafana", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != grafanaSnapshotVersion {
		logger.Load(ctx).WarnContext(ctx,
			"grafana: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", grafanaSnapshotVersion)

		b.registry.ResetAll()
		b.nextServiceAccountID = 0
		b.nextTokenID = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("grafana: restore tables: %w", err)
	}

	restoreWorkspaceTagsLocked(b)

	b.accountID = snap.AccountID
	b.region = snap.Region
	b.nextServiceAccountID = snap.NextServiceAccountID
	b.nextTokenID = snap.NextTokenID

	return nil
}

// restoreWorkspaceTagsLocked rebuilds a Prometheus-named tags.Tags instance
// for every workspace after a JSON round-trip, which deserializes every
// tags.Tags with the generic "json.tags" name (see tags.Tags.UnmarshalJSON's
// doc comment) -- matching services/eks's restoreClusterAndNodegroupTagsLocked.
// Callers must hold b.mu.
func restoreWorkspaceTagsLocked(b *InMemoryBackend) {
	for _, w := range b.workspaces.All() {
		if w.Tags == nil {
			w.Tags = tags.New("grafana.workspace." + w.ID + ".tags")

			continue
		}

		raw := w.Tags.Clone()
		w.Tags.Close()
		w.Tags = tags.FromMap("grafana.workspace."+w.ID+".tags", raw)
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
