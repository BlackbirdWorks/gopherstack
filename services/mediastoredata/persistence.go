package mediastoredata

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// mediastoredataSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to Object or backendSnapshot itself would
// make an older snapshot unsafe to decode as the current shape. Restore
// compares this against the persisted value and discards (b.states reset to
// empty, not a partial decode) any mismatch -- see Restore below. This is the
// first version: MediaStore Data had no persistence at all before Phase 3.3
// (neither Handler nor InMemoryBackend implemented Snapshot/Restore, so
// cli.go's generic setupPersistence never picked it up -- see the
// Handler.Snapshot/Restore delegation at the bottom of this file), so there
// is no legacy snapshot shape to be compatible with -- any snapshot without a
// matching Version (including one with no version field, which decodes as 0)
// is discarded the same way any other incompatible snapshot is.
const mediastoredataSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the MediaStore Data
// backend.
//
// States is nested per-region (region -> that region's deterministic
// [store.Table.Snapshot] slice), matching services/mediastore's
// region-nested-map pattern: the set of regions is only known at runtime, so
// b.states is NOT registered on a *store.Registry (Registry's
// SnapshotAll/RestoreAll require a fixed, construction-time-known
// table-name set) and is captured/restored directly here instead. See
// store_setup.go for why states is region-nested in the first place.
type backendSnapshot struct {
	States  map[string][]*Object `json:"states"`
	Version int                  `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	states := make(map[string][]*Object, len(b.states))
	for region, tbl := range b.states {
		states[region] = tbl.Snapshot()
	}

	snap := backendSnapshot{
		Version: mediastoredataSnapshotVersion,
		States:  states,
	}

	return persistence.MarshalSnapshot(ctx, "mediastoredata", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "mediastoredata", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != mediastoredataSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"mediastoredata: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", mediastoredataSnapshotVersion)

		b.states = make(map[string]*store.Table[Object])

		return nil
	}

	b.states = make(map[string]*store.Table[Object])
	for region, items := range snap.States {
		b.state(region).Restore(items)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// MediaStore Data had no persistence at all before Phase 3.3 -- neither
// Handler nor InMemoryBackend implemented Snapshot/Restore, so cli.go's
// generic setupPersistence (which type-asserts the registered
// service.Registerable, i.e. the Handler, for a Snapshot/Restore pair) never
// picked MediaStore Data up: dead wiring, with no persistence underneath it
// either. This delegation (matching the codecommit/cleanrooms/mediastore
// pattern) wires MediaStore Data into persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
