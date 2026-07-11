package cleanrooms

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// cleanroomsSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as
// the current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting tagsByArn, not a partial
// decode) any mismatch -- see Restore below. This is the first version:
// CleanRooms had no persistence at all before Phase 3.3 (Handler had no
// Snapshot/Restore, so cli.go's generic setupPersistence never picked it up
// even though nothing on the backend implemented persistence.Persistable
// either -- see the Handler.Snapshot/Restore delegation at the bottom of
// this file, and the Snapshot/Restore doc comment on StorageBackend in
// interfaces.go), so there is no legacy snapshot shape to be compatible
// with -- any snapshot without a matching Version (including one with no
// version field, which decodes as 0) is discarded the same way any other
// incompatible snapshot is.
const cleanroomsSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the CleanRooms backend.
//
// Tables holds one JSON-encoded array per registered table name, produced
// directly by b.registry.SnapshotAll(): every converted resource collection
// derives its store.Table key entirely from real, already-wire-visible
// fields on the value type (see store_setup.go), so none of them need a DTO
// wrapper -- unlike services/workmail or services/codeartifact.
//
// TagsByArn is the one remaining raw (non-store.Table) map: its values are
// plain map[string]string, not *T, so there is nothing for store.Table to
// key on (see backend.go's InMemoryBackend field doc comment). It is
// persisted directly here.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	TagsByArn map[string]map[string]string `json:"tagsByArn"`
	AccountID string                       `json:"accountID"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "cleanrooms: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   cleanroomsSnapshotVersion,
		Tables:    tables,
		TagsByArn: b.tagsByArn,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "cleanrooms", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "cleanrooms", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != cleanroomsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"cleanrooms: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", cleanroomsSnapshotVersion)

		b.registry.ResetAll()
		b.tagsByArn = make(map[string]map[string]string)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("cleanrooms: restore snapshot tables: %w", err)
	}

	if snap.TagsByArn == nil {
		snap.TagsByArn = make(map[string]map[string]string)
	}
	b.tagsByArn = snap.TagsByArn

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked CleanRooms up at all: dead wiring,
// with no persistence underneath it either. This delegation (matching the
// codecommit/codepipeline/emr/workmail pattern) is what wires CleanRooms
// into persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
