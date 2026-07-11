package opsworks

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// opsworksSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to backendSnapshot (or a value type held by one
// of the registered tables) would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (ResetAll, not a partial decode) any mismatch -- see Restore. The
// pre-Phase-3.3 snapshot format had no version field at all, so an old
// snapshot decodes with Version == 0, which is guaranteed to mismatch
// opsworksSnapshotVersion and is discarded the same way any other
// incompatible snapshot is.
const opsworksSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the OpsWorks backend.
//
// Tables holds one JSON-encoded array per registered table name, produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field is a
// "clean" table (see store_setup.go's file doc comment), so no ephemeral DTO
// registry is needed here. Tags is the one map left un-converted (its values
// are plain string maps, not *T). Version guards against decoding a snapshot
// from an incompatible (older or newer) build of this backend as though it
// were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountID"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are all plain JSON-friendly structs, so a
		// marshal failure here would indicate a programming error rather
		// than bad input data. Log and skip the snapshot rather than panic,
		// matching the persistence.Persistable contract (nil is skipped by
		// the Manager).
		logger.Load(ctx).WarnContext(ctx, "opsworks: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   opsworksSnapshotVersion,
		Tables:    tables,
		Tags:      b.tags,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "opsworks", &snap)
}

// Restore deserializes backend state from a snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "opsworks", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != opsworksSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"opsworks: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", opsworksSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("opsworks: restore snapshot tables: %w", err)
	}

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string]string)
	}

	b.tags = snap.Tags
	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
//
// Prior to Phase 3.3, Handler had no Snapshot/Restore of its own even though
// InMemoryBackend implemented both (dead wiring: nothing ever called them).
// This delegation is what actually wires OpsWorks into the persistence
// Manager.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
// See the Snapshot doc comment above for why this delegation is new.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
