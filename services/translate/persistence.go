package translate

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// translateSnapshotVersion identifies the shape of [backendSnapshot]. It must
// be bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll plus resetting tags, not a partial decode) any
// mismatch -- see Restore below. This is the first version: Translate had no
// persistence at all before Phase 3.3 (Handler had no Snapshot/Restore, and
// nothing on the backend implemented persistence.Persistable either -- see
// the Handler.Snapshot/Restore delegation at the bottom of this file), so
// there is no legacy snapshot shape to be compatible with -- any snapshot
// without a matching Version (including one with no version field, which
// decodes as 0) is discarded the same way any other incompatible snapshot
// is.
const translateSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the Translate backend.
//
// Tables holds one JSON-encoded array per registered table name
// (terminologies, parallelData, jobs; see store_setup.go), produced directly
// by b.registry.SnapshotAll(): every converted resource collection derives
// its store.Table key entirely from a real, already-wire-visible field on
// the value type, so none of them need a DTO wrapper.
//
// Tags is the one remaining raw (non-store.Table) map: its values are plain
// map[string]string, not *T, so there is nothing for store.Table to key on
// (see backend.go's InMemoryBackend field doc comment). It is persisted
// directly here.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage   `json:"tables"`
	Tags      map[string]map[string]string `json:"tags"`
	AccountID string                       `json:"accountId"`
	Region    string                       `json:"region"`
	Version   int                          `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock()
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "translate: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   translateSnapshotVersion,
		Tables:    tables,
		Tags:      b.tags,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "translate", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "translate", data, &snap); err != nil {
		return err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if snap.Version != translateSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"translate: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", translateSnapshotVersion)

		b.registry.ResetAll()
		b.tags = make(map[string]map[string]string)

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("translate: restore snapshot tables: %w", err)
	}

	b.tags = snap.Tags
	if b.tags == nil {
		b.tags = make(map[string]map[string]string)
	}

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// Handler previously had no Snapshot/Restore of its own -- and neither did
// InMemoryBackend -- so cli.go's generic setupPersistence (which
// type-asserts the registered service.Registerable, i.e. the Handler, for a
// Snapshot/Restore pair) never picked Translate up at all: dead wiring, with
// no persistence underneath it either. This delegation (matching the
// codecommit/polly/cleanrooms pattern) is what wires Translate into
// persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
