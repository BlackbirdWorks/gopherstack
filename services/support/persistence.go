package support

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// supportSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a DTO type, a registered table's value type,
// or backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore. The pre-Phase-3.3 snapshot format had no version field at all, so
// an old snapshot decodes with Version == 0, which is guaranteed to mismatch
// supportSnapshotVersion and is discarded the same way any other incompatible
// snapshot is.
const supportSnapshotVersion = 1

// attachmentSetSnapshot is a DTO used only for Snapshot/Restore of the
// "dirty" attachmentSets table -- see store_setup.go's file doc comment for
// why it can't be registered directly on b.registry. It wraps the live value
// alongside the ID field that value intentionally excludes from JSON
// (json:"-"), so the ID survives the round trip.
type attachmentSetSnapshot struct {
	ID  string        `json:"id"`
	Set AttachmentSet `json:"set"`
}

func attachmentSetSnapshotKey(v *attachmentSetSnapshot) string { return v.ID }

// newAttachmentSetDTORegistry builds the ephemeral DTO [store.Registry]
// shared by Snapshot and Restore for the attachmentSets table (see
// store_setup.go).
func newAttachmentSetDTORegistry() (*store.Registry, *store.Table[attachmentSetSnapshot]) {
	dtoReg := store.NewRegistry()
	asDTOs := store.Register(dtoReg, "attachmentSets", store.New(attachmentSetSnapshotKey))

	return dtoReg, asDTOs
}

// backendSnapshot is the top-level on-disk shape for the Support backend.
//
// Tables holds one JSON-encoded array per registered table name: the four
// "clean" tables on b.registry (cases, attachments, checkRefreshStatuses,
// checkResults) plus the DTO table built above for the "dirty" one
// (attachmentSets) -- see store_setup.go's registerAllTables doc for the
// clean/dirty split. Communications (caseID -> chronological communications)
// is order-sensitive and left a plain map -- see store_setup.go -- so it is
// persisted here directly instead of through a store.Table.
type backendSnapshot struct {
	Tables         map[string]json.RawMessage `json:"tables"`
	Communications map[string][]Communication `json:"communications"`
	NextDisplayID  uint64                     `json:"nextDisplayId"`
	Version        int                        `json:"version"`
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "support: snapshot table marshal failed", "error", err)

		return nil
	}

	dtoReg, asDTOs := newAttachmentSetDTORegistry()

	for _, v := range b.attachmentSets.Snapshot() {
		asDTOs.Put(&attachmentSetSnapshot{ID: v.ID, Set: *v})
	}

	dirtyTables, err := dtoReg.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "support: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dirtyTables)

	snap := backendSnapshot{
		Version:        supportSnapshotVersion,
		Tables:         tables,
		Communications: b.communications,
		NextDisplayID:  b.nextDisplayID,
	}

	data, err := json.Marshal(snap)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "support: snapshot marshal failed", "error", err)

		return nil
	}

	return data
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "support", data, &snap); err != nil {
		return err
	}

	if b.mu == nil {
		b.mu = lockmetrics.New("support")
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != supportSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"support: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", supportSnapshotVersion)

		b.resetTablesLocked()
		b.communications = make(map[string][]Communication)
		b.nextDisplayID = 0

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("support: restore snapshot tables: %w", err)
	}

	if err := b.restoreAttachmentSetsLocked(snap.Tables); err != nil {
		return err
	}

	if snap.Communications == nil {
		snap.Communications = make(map[string][]Communication)
	}

	b.communications = snap.Communications
	b.nextDisplayID = snap.NextDisplayID

	return nil
}

// restoreAttachmentSetsLocked rebuilds the "dirty" attachmentSets table from
// tables via the ephemeral DTO registry, converting each DTO back to its live
// type. The caller MUST hold b.mu for writing.
func (b *InMemoryBackend) restoreAttachmentSetsLocked(tables map[string]json.RawMessage) error {
	dtoReg, asDTOs := newAttachmentSetDTORegistry()
	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("support: restore snapshot DTO tables: %w", err)
	}

	live := make([]*AttachmentSet, 0, asDTOs.Len())

	for _, dto := range asDTOs.All() {
		set := dto.Set
		set.ID = dto.ID
		live = append(live, &set)
	}

	b.attachmentSets.Restore(live)

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
