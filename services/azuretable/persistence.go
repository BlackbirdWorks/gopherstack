package azuretable

import (
	"context"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
)

// azureTableSnapshotVersion identifies the shape of backendSnapshot. Must be
// bumped whenever a change to storedTable/storedEntity would make an older
// snapshot unsafe to decode as the current shape; Restore compares this
// against the persisted value and discards (rather than partially decodes)
// any mismatch, mirroring services/azurequeue and services/azureblob.
//
// Bumped from 1 to 2 for two incompatible shape changes made in the same
// pass: (1) EntityProperty's Edm.Int64 wire value moved from a bare float64
// JSON number (which silently lost precision above 2^53) to a decimal
// string, and (2) storedTable.Entities' map key moved from a delimited
// string ("partitionKey\x00rowKey") to entityCompositeKey (a struct,
// persisted via its own MarshalText as a JSON string array) to close a
// NUL-byte delimiter collision. Both changes decode a version-1 snapshot
// incorrectly if not gated behind a version check -- pkgs/persistence's
// TestSnapshotVersionGuard enforces exactly this: an incompatible retype
// must pair with a version bump, purely additive field growth must not.
const azureTableSnapshotVersion = 2

// backendSnapshot is the top-level on-disk shape for the Azure Table
// backend. Tables serialises directly (no DTO layer): storedTable/
// storedEntity have no unexported fields, so encoding/json round-trips them
// as-is (EntityProperty's own MarshalJSON/UnmarshalJSON handle its typed
// Value field -- see models.go).
type backendSnapshot struct {
	Tables  map[string]*storedTable `json:"tables"`
	Version int                     `json:"version"`
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	snap := backendSnapshot{
		Version: azureTableSnapshotVersion,
		Tables:  b.tables,
	}

	return persistence.MarshalSnapshot(ctx, "azuretable", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "azuretable", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != azureTableSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- discard cleanly and start
		// empty instead of erroring, since this is an expected, recoverable
		// condition (e.g. upgrading gopherstack across a snapshot-format
		// change), not data corruption. Mirrors services/azurequeue and
		// services/azureblob.
		logger.Load(ctx).WarnContext(ctx,
			"azuretable: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", azureTableSnapshotVersion)

		b.tables = make(map[string]*storedTable)

		return nil
	}

	if snap.Tables == nil {
		snap.Tables = make(map[string]*storedTable)
	}

	if err := validateSnapshotTables(snap.Tables); err != nil {
		return err
	}

	b.tables = snap.Tables

	return nil
}

// validateSnapshotTables rejects a snapshot whose "tables" map (or any
// table's "Entities" map) holds a JSON null entry -- which decodes to a nil
// pointer that would panic on first dereference if stored as-is -- or whose
// map key disagrees with the entry's own Name field, mirroring
// services/azurequeue's identical Restore validation. It also initializes
// any table whose own "Entities" map is JSON `null` (legal JSON, decodes to
// a nil Go map, not a nil pointer -- so it isn't rejected above) to an empty
// map: a nil map is safe to range over and read from, but assigning into
// one (as InsertEntity/ReplaceEntity/MergeEntity all do) panics. Mirrors the
// same nil-map init this function's caller already does for a nil top-level
// "tables" map.
func validateSnapshotTables(tables map[string]*storedTable) error {
	for name, t := range tables {
		if t == nil {
			return fmt.Errorf("%w: %q", ErrSnapshotTableNull, name)
		}

		if t.Name != name {
			return fmt.Errorf("%w: map key %q, Name %q", ErrSnapshotTableNameMismatch, name, t.Name)
		}

		for key, e := range t.Entities {
			if e == nil {
				return fmt.Errorf("%w: key %v in table %q", ErrSnapshotEntityNull, key, name)
			}
		}

		if t.Entities == nil {
			t.Entities = make(map[entityCompositeKey]*storedEntity)
		}
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	type snapshotter interface {
		Snapshot(ctx context.Context) []byte
	}
	if s, ok := h.Backend.(snapshotter); ok {
		return s.Snapshot(ctx)
	}

	return nil
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	type restorer interface {
		Restore(context.Context, []byte) error
	}
	if r, ok := h.Backend.(restorer); ok {
		if err := r.Restore(ctx, data); err != nil {
			return fmt.Errorf("azuretable: restore snapshot: %w", err)
		}
	}

	return nil
}
