package applicationautoscaling

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// applicationautoscalingSnapshotVersion identifies the shape of
// [backendSnapshot]. It must be bumped whenever a change to backendSnapshot
// (or a value type held by one of the registered tables) would make an older
// snapshot unsafe to decode as the current shape. Restore compares this
// against the persisted value and discards (registry.ResetAll, not a partial
// decode) any mismatch -- see Restore. The pre-Phase-3.3 snapshot format had
// no version field at all, so an old snapshot decodes with Version == 0,
// which is guaranteed to mismatch applicationautoscalingSnapshotVersion and
// is discarded the same way any other incompatible snapshot is.
const applicationautoscalingSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Application Auto
// Scaling backend.
//
// Tables holds one JSON-encoded array per registered table name (see
// store_setup.go's registerAllTables): scalableTargets, scalingPolicies, and
// scheduledActions. All three are "clean" tables keyed directly by a real
// field of the resource (see store_setup.go's file doc comment), so no
// ephemeral DTO-registry is needed here.
//
// scalingActivities is intentionally NOT part of this snapshot: it was never
// persisted before this refactor (the pre-Phase-3.3 backendSnapshot omitted
// it too), so it remains ephemeral, reset to empty by both Reset and a
// version-mismatch Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "applicationautoscaling: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   applicationautoscalingSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "applicationautoscaling", snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "applicationautoscaling", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != applicationautoscalingSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"applicationautoscaling: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", applicationautoscalingSnapshotVersion)

		b.registry.ResetAll()
		b.scalingActivities = nil

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("applicationautoscaling: restore snapshot tables: %w", err)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
