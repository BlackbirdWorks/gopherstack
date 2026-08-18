package polly

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// pollySnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to a registered table's value type or
// backendSnapshot itself would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (registry.ResetAll, not a partial decode) any mismatch -- see
// Restore below. Version 1 was the first version: Polly had no persistence at
// all before Phase 3.3 (Handler had no Snapshot/Restore, and nothing on the
// backend implemented persistence.Persistable either). Version 2 dropped the
// Tags field when the TagResource/UntagResource/ListTagsForResource surface
// was removed (that surface was never part of the real Amazon Polly API --
// see PARITY.md); any snapshot without a matching Version (including one with
// no version field, which decodes as 0) is discarded the same way any other
// incompatible snapshot is.
const pollySnapshotVersion = 2

// backendSnapshot is the top-level on-disk shape for the Polly backend.
//
// Tables holds one JSON-encoded array per registered table name (lexicons,
// tasks; see store_setup.go), produced directly by b.registry.SnapshotAll():
// both tables derive their store.Table key entirely from a real,
// already-wire-visible field on the value type, so neither needs a DTO
// wrapper.
//
// The built-in voice catalogue (InMemoryBackend.voices) is static read-only
// data, not a mutable resource collection, and is intentionally excluded.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountId"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "polly: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:   pollySnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "polly", snap)
}

// Restore deserializes backend state from a snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "polly", data, &snap); err != nil {
		return err
	}

	if b.mu == nil {
		b.mu = lockmetrics.New("polly")
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != pollySnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"polly: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", pollySnapshotVersion)

		b.registry.ResetAll()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("polly: restore snapshot tables: %w", err)
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
