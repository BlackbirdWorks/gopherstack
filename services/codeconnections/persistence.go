package codeconnections

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// codeconnectionsSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to a DTO type, a registered table's value
// type, or backendSnapshot itself would make an older snapshot unsafe to
// decode as the current shape. Restore compares this against the persisted
// value and discards (registry.ResetAll plus the two dirty tables' own
// Reset, not a partial decode) any mismatch -- see Restore below. This is the
// first version: the pre-Phase-3.3 snapshot format had no version field at
// all (plain region-nested resource maps), so any snapshot without a
// matching Version (including one with no version field, which decodes as 0)
// is discarded the same way any other incompatible snapshot is.
const codeconnectionsSnapshotVersion = 1

// regionalDTO wraps a region-nested resource whose own identity is a single
// string recoverable from an existing key helper (RepositoryLink.
// RepositoryLinkID; SyncConfiguration's ResourceName+SyncType via
// syncConfigKey) for JSON round-tripping through store.Registry.
// Table[V].Snapshot's plain json.Marshal(V) cannot see the unexported
// `region` field each of those two types carries (used only for the live
// table's composite key -- see store_setup.go), so it is carried alongside
// Value here instead. ID lets the DTO table itself have a stable composite
// key ("Region|ID") via one generic keyFn, without needing per-type
// key-derivation logic.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the CodeConnections backend.
//
// Tables holds one JSON-encoded array per store.Table: "connections" and
// "hosts" come straight from b.registry.SnapshotAll() (clean, no DTO
// needed); "repositoryLinks" and "syncConfigurations" come from an ephemeral
// DTO registry built fresh in Snapshot/Restore (dirty -- see
// store_setup.go), merged into the same map. Version guards against decoding
// a snapshot from an incompatible (older or newer) build of this backend as
// though it were the current shape; see Restore.
type backendSnapshot struct {
	Tables    map[string]json.RawMessage `json:"tables"`
	AccountID string                     `json:"accountID"`
	Region    string                     `json:"region"`
	Version   int                        `json:"version"`
}

// persistenceDTOTables groups the ephemeral DTO tables Snapshot/Restore
// construct for the two dirty tables, so they can be passed around as one
// value instead of two separate return values.
type persistenceDTOTables struct {
	registry           *store.Registry
	repositoryLinks    *store.Table[regionalDTO[RepositoryLink]]
	syncConfigurations *store.Table[regionalDTO[SyncConfiguration]]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types differ from the live
// dirty-table value types.
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry: dtoReg,
		repositoryLinks: store.Register(
			dtoReg, "repositoryLinks", store.New(regionalDTOKeyFn[RepositoryLink]),
		),
		syncConfigurations: store.Register(
			dtoReg, "syncConfigurations", store.New(regionalDTOKeyFn[SyncConfiguration]),
		),
	}
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "codeconnections: snapshot table marshal failed", "error", err)

		return nil
	}

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.repositoryLinks.Snapshot() {
		dtos.repositoryLinks.Put(&regionalDTO[RepositoryLink]{Value: v, Region: v.region, ID: v.RepositoryLinkID})
	}

	for _, v := range b.syncConfigurations.Snapshot() {
		dtos.syncConfigurations.Put(&regionalDTO[SyncConfiguration]{
			Value: v, Region: v.region, ID: syncConfigKey(v.ResourceName, v.SyncType),
		})
	}

	dtoTables, err := dtos.registry.SnapshotAll()
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "codeconnections: snapshot DTO table marshal failed", "error", err)

		return nil
	}

	maps.Copy(tables, dtoTables)

	snap := backendSnapshot{
		Version:   codeconnectionsSnapshotVersion,
		Tables:    tables,
		AccountID: b.accountID,
		Region:    b.defaultRegion,
	}

	return persistence.MarshalSnapshot(ctx, "codeconnections", &snap)
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "codeconnections", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != codeconnectionsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"codeconnections: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", codeconnectionsSnapshotVersion)

		b.registry.ResetAll()
		b.repositoryLinks.Reset()
		b.syncConfigurations.Reset()
		b.accountID = snap.AccountID
		b.defaultRegion = snap.Region

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return fmt.Errorf("codeconnections: restore snapshot tables: %w", err)
	}

	if err := b.restoreDirtyTables(snap.Tables); err != nil {
		return err
	}

	b.accountID = snap.AccountID
	b.defaultRegion = snap.Region

	return nil
}

// restoreDirtyTables rebuilds the two dirty tables (repositoryLinks,
// syncConfigurations; see store_setup.go's registerAllTables doc) from
// tables via the ephemeral DTO registry, factored out of Restore to keep
// Restore's own cognitive complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreDirtyTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()
	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("codeconnections: restore snapshot DTO tables: %w", err)
	}

	b.repositoryLinks.Reset()

	for _, d := range dtos.repositoryLinks.Snapshot() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		b.repositoryLinks.Put(d.Value)
	}

	b.syncConfigurations.Reset()

	for _, d := range dtos.syncConfigurations.Snapshot() {
		if d.Value == nil {
			continue
		}

		d.Value.region = d.Region
		b.syncConfigurations.Put(d.Value)
	}

	return nil
}
