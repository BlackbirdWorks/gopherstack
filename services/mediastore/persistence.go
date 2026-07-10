package mediastore

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// mediastoreSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to Container or backendSnapshot itself
// would make an older snapshot unsafe to decode as the current shape.
// Restore compares this against the persisted value and discards
// (b.containers reset to empty, not a partial decode) any mismatch -- see
// Restore below. This is the first version: MediaStore had no persistence at
// all before Phase 3.3 (neither Handler nor InMemoryBackend implemented
// Snapshot/Restore, so cli.go's generic setupPersistence never picked it up
// -- see the Handler.Snapshot/Restore delegation at the bottom of this
// file), so there is no legacy snapshot shape to be compatible with -- any
// snapshot without a matching Version (including one with no version field,
// which decodes as 0) is discarded the same way any other incompatible
// snapshot is.
const mediastoreSnapshotVersion = 1

// backendSnapshot is the top-level on-disk shape for the MediaStore backend.
//
// Containers is nested per-region (region -> that region's deterministic
// [store.Table.Snapshot] slice), matching services/elasticache's
// region-nested-map pattern: the set of regions is only known at runtime, so
// b.containers is NOT registered on a *store.Registry (Registry's
// SnapshotAll/RestoreAll require a fixed, construction-time-known
// table-name set) and is captured/restored directly here instead. See
// store_setup.go for why containers is region-nested in the first place.
//
// paginationSecret is deliberately NOT persisted, matching the sibling
// AppConfig/AppConfigData services: it is regenerated fresh on every process
// start (see NewInMemoryBackend). A consequence is that any ListContainers
// NextToken issued before a restore will fail its HMAC check on first use
// afterward -- the same "secret doesn't survive a restart" limitation those
// services already accept for their own pagination/session tokens, not a new
// one introduced here.
type backendSnapshot struct {
	Containers map[string][]*Container `json:"containers"`
	Version    int                     `json:"version"`
}

// Snapshot serializes the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	containers := make(map[string][]*Container, len(b.containers))
	for region, tbl := range b.containers {
		containers[region] = tbl.Snapshot()
	}

	snap := backendSnapshot{
		Version:    mediastoreSnapshotVersion,
		Containers: containers,
	}

	return persistence.MarshalSnapshot(ctx, "mediastore", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot
	if err := persistence.UnmarshalSnapshot(ctx, "mediastore", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != mediastoreSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"mediastore: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", mediastoreSnapshotVersion)

		b.containers = make(map[string]*store.Table[Container])

		return nil
	}

	b.containers = make(map[string]*store.Table[Container])
	for region, items := range snap.Containers {
		b.containersStore(region).Restore(items)
	}

	return nil
}

// Snapshot implements persistence.Persistable by delegating to the backend.
// MediaStore had no persistence at all before Phase 3.3 -- neither Handler
// nor InMemoryBackend implemented Snapshot/Restore, so cli.go's generic
// setupPersistence (which type-asserts the registered service.Registerable,
// i.e. the Handler, for a Snapshot/Restore pair) never picked MediaStore up:
// dead wiring, with no persistence underneath it either. This delegation
// (matching the codecommit/cleanrooms pattern) wires MediaStore into
// persistence for the first time.
func (h *Handler) Snapshot(ctx context.Context) []byte {
	return h.Backend.Snapshot(ctx)
}

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
