package ram

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// ramSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to backendSnapshot (or a value type held by one of
// the registered tables) would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all
// (it serialised resourceShares/permissions/invitations as bare JSON
// objects rather than the "tables" wrapper below), so an old snapshot
// decodes with Version == 0, which is guaranteed to mismatch
// ramSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
const ramSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the RAM backend.
//
// Tables holds one JSON-encoded array per registered table name
// (resourceShares, permissions, invitations), produced by
// b.registry.SnapshotAll() -- every store.Table-backed resource field here
// is a "clean" table (see store_setup.go's file doc comment), so no
// ephemeral DTO registry is needed. sharePermissions and associations are
// not store.Table-backed (see store_setup.go) and remain persisted directly.
// Version guards against decoding a snapshot from an incompatible (older or
// newer) build of this backend as though it were the current shape; see
// Restore.
type backendSnapshot struct {
	Tables           map[string]json.RawMessage  `json:"tables"`
	SharePermissions map[string]map[string]int32 `json:"sharePermissions"`
	AccountID        string                      `json:"accountID"`
	Region           string                      `json:"region"`
	Associations     []*ResourceShareAssociation `json:"associations"`
	Version          int                         `json:"version"`
}

// ensureNonNilMaps initialises nil maps/slices in the snapshot to empty
// collections so downstream code never has to nil-check them.
func ensureNonNilMaps(snap *backendSnapshot) {
	if snap.SharePermissions == nil {
		snap.SharePermissions = make(map[string]map[string]int32)
	}

	if snap.Associations == nil {
		snap.Associations = make([]*ResourceShareAssociation, 0)
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ram: snapshot table marshal failed", "error", err)

		return nil
	}

	// Deep-copy nested sharePermissions map.
	sharePermissions := make(map[string]map[string]int32, len(b.sharePermissions))

	for k, v := range b.sharePermissions {
		sharePermissions[k] = maps.Clone(v)
	}

	// Copy associations slice.
	associations := make([]*ResourceShareAssociation, len(b.associations))
	copy(associations, b.associations)

	snap := backendSnapshot{
		Version:          ramSnapshotVersion,
		Tables:           tables,
		SharePermissions: sharePermissions,
		Associations:     associations,
		AccountID:        b.accountID,
		Region:           b.region,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "ram: failed to snapshot backend state", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "ram", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != ramSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"ram: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", ramSnapshotVersion)

		b.registry.ResetAll()
		b.associations = make([]*ResourceShareAssociation, 0)
		b.sharePermissions = make(map[string]map[string]int32)
		b.seedBuiltInPermissions()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("ram: restore snapshot tables: %w", err)
	}

	ensureNonNilMaps(&snap)

	b.associations = snap.Associations
	b.sharePermissions = snap.SharePermissions
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
