package efs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// efsSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to regionalDTO/backendSnapshot itself would make
// an older snapshot unsafe to decode as the current shape (e.g. a field's
// meaning or type changes). Restore compares this against the persisted
// value and discards (rather than attempts to partially decode) any mismatch
// -- see Restore below. This is the first version: pre-Phase-3.3 snapshots
// carried no version field at all, so they also fail this check and are
// discarded rather than misread, which is the safe behaviour across any
// snapshot-format change.
const efsSnapshotVersion = 1

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. store.Table[V].Snapshot's plain json.Marshal(V) cannot see
// the unexported `region` field MountTarget/AccessPoint carry (used only for
// the live table's composite key -- see store_setup.go), so it is carried
// alongside Value here instead. ID mirrors the resource's own identifier so
// the DTO table itself has a stable composite key ("region|ID") independent
// of which concrete V it wraps.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the EFS backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll]: fileSystems and replicationConfigs are
// registered directly under their own live keyFn (both already carry their
// owning region visibly in JSON -- FileSystem.Region /
// ReplicationConfiguration.SourceFileSystemRegion -- so no wrapper is
// needed), while mountTargets and accessPoints are wrapped in regionalDTO to
// carry their hidden `region` field through. LifecyclePolicies/
// BackupPolicies/FileSystemPolicies are still region-nested plain maps (see
// store_setup.go for why they were not converted to store.Table) and are
// persisted directly, as before. Version guards against decoding a snapshot
// from an incompatible (older or newer) build of this backend as though it
// were the current shape; see Restore.
type backendSnapshot struct {
	Tables             map[string]json.RawMessage              `json:"tables"`
	LifecyclePolicies  map[string]map[string][]LifecyclePolicy `json:"lifecyclePolicies"`
	BackupPolicies     map[string]map[string]string            `json:"backupPolicies"`
	FileSystemPolicies map[string]map[string]string            `json:"fileSystemPolicies"`
	AccountID          string                                  `json:"accountID"`
	Region             string                                  `json:"region"`
	Version            int                                     `json:"version"`
}

// persistenceDTOTables groups every ephemeral DTO table
// buildPersistenceDTORegistry constructs, so Snapshot/Restore can pass them
// around as one value instead of four separate return values.
type persistenceDTOTables struct {
	registry           *store.Registry
	fileSystems        *store.Table[FileSystem]
	mountTargets       *store.Table[regionalDTO[MountTarget]]
	accessPoints       *store.Table[regionalDTO[AccessPoint]]
	replicationConfigs *store.Table[ReplicationConfiguration]
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types differ from the live table
// value types for mountTargets/accessPoints (regionalDTO[V] vs V).
func buildPersistenceDTORegistry() persistenceDTOTables {
	dtoReg := store.NewRegistry()

	return persistenceDTOTables{
		registry:           dtoReg,
		fileSystems:        store.Register(dtoReg, "fileSystems", store.New(fileSystemKeyFn)),
		mountTargets:       store.Register(dtoReg, "mountTargets", store.New(regionalDTOKeyFn[MountTarget])),
		accessPoints:       store.Register(dtoReg, "accessPoints", store.New(regionalDTOKeyFn[AccessPoint])),
		replicationConfigs: store.Register(dtoReg, "replicationConfigs", store.New(replicationConfigKeyFn)),
	}
}

// Snapshot serialises the backend state to JSON.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtos := buildPersistenceDTORegistry()

	for _, v := range b.fileSystems.Snapshot() {
		dtos.fileSystems.Put(v)
	}
	for _, v := range b.mountTargets.Snapshot() {
		dtos.mountTargets.Put(&regionalDTO[MountTarget]{Region: v.region, ID: v.MountTargetID, Value: v})
	}
	for _, v := range b.accessPoints.Snapshot() {
		dtos.accessPoints.Put(&regionalDTO[AccessPoint]{Region: v.region, ID: v.AccessPointID, Value: v})
	}
	for _, v := range b.replicationConfigs.Snapshot() {
		dtos.replicationConfigs.Put(v)
	}

	tables, err := dtos.registry.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the
		// Manager).
		logger.Load(ctx).WarnContext(ctx, "efs: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:            efsSnapshotVersion,
		Tables:             tables,
		LifecyclePolicies:  b.lifecyclePolicies,
		BackupPolicies:     b.backupPolicies,
		FileSystemPolicies: b.fileSystemPolicies,
		AccountID:          b.accountID,
		Region:             b.region,
	}

	return persistence.MarshalSnapshot(ctx, "efs", snap)
}

// Restore loads backend state from a JSON snapshot produced by Snapshot.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "efs", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != efsSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields (e.g. the pre-Phase-3.3 nested
		// map[string]map[string]*T shape vs. the flat store.Table
		// composite-key shape). Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"efs: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", efsSnapshotVersion)

		b.registry.ResetAll()
		b.fileSystemsByARN.Reset()
		b.mountTargetsByARN.Reset()
		b.accessPointsByARN.Reset()
		b.accessPointsByClientToken.Reset()
		b.initRegionMaps()
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	if snap.LifecyclePolicies == nil {
		snap.LifecyclePolicies = make(map[string]map[string][]LifecyclePolicy)
	}
	b.lifecyclePolicies = snap.LifecyclePolicies

	if snap.BackupPolicies == nil {
		snap.BackupPolicies = make(map[string]map[string]string)
	}
	b.backupPolicies = snap.BackupPolicies

	if snap.FileSystemPolicies == nil {
		snap.FileSystemPolicies = make(map[string]map[string]string)
	}
	b.fileSystemPolicies = snap.FileSystemPolicies

	b.accountID = snap.AccountID
	b.region = snap.Region

	b.rebuildDerivedIndexes()

	return nil
}

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtos := buildPersistenceDTORegistry()

	if err := dtos.registry.RestoreAll(tables); err != nil {
		return fmt.Errorf("efs: restore snapshot tables: %w", err)
	}

	b.fileSystems.Restore(dtos.fileSystems.Snapshot())
	b.mountTargets.Restore(unwrapRegionalDTOs(dtos.mountTargets, func(v *MountTarget, r string) { v.region = r }))
	b.accessPoints.Restore(unwrapRegionalDTOs(dtos.accessPoints, func(v *AccessPoint, r string) { v.region = r }))
	b.replicationConfigs.Restore(dtos.replicationConfigs.Snapshot())

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the region assignment is
// supplied by the caller, one per concrete V; see restoreResourceTables). A
// DTO whose Value is nil -- not producible by Snapshot, but a defensive
// guard against a hand-edited or corrupted snapshot -- is skipped rather
// than dereferenced.
func unwrapRegionalDTOs[V any](dtos *store.Table[regionalDTO[V]], setRegion func(*V, string)) []*V {
	items := make([]*V, 0, dtos.Len())

	for _, d := range dtos.All() {
		if d.Value == nil {
			continue
		}

		setRegion(d.Value, d.Region)
		items = append(items, d.Value)
	}

	return items
}

// rebuildDerivedIndexes reconstructs every derived-cache table
// (fileSystemsByARN/mountTargetsByARN/accessPointsByARN/
// accessPointsByClientToken) and every non-*T auxiliary map
// (creationTokenIdx, mtSubnetIdx, apByFS) from the just-restored live
// tables, and reinitialises any nil tag registry (defensive, against a
// hand-edited or corrupted snapshot). Callers must hold b.mu.Lock.
func (b *InMemoryBackend) rebuildDerivedIndexes() {
	b.rebuildFileSystemIndexes()
	b.rebuildMountTargetIndexes()
	b.rebuildAccessPointIndexes()
}

func (b *InMemoryBackend) rebuildFileSystemIndexes() {
	b.fileSystemsByARN.Reset()
	b.creationTokenIdx = make(map[string]map[string]string)

	for _, fs := range b.fileSystems.All() {
		if fs.Tags == nil {
			fs.Tags = tags.New("efs.filesystem." + fs.FileSystemID + ".tags")
		}

		b.fileSystemsByARN.Put(fs)

		if fs.CreationToken != "" {
			b.tokenIdxStore(fs.Region)[fs.CreationToken] = fs.FileSystemID
		}
	}
}

func (b *InMemoryBackend) rebuildMountTargetIndexes() {
	b.mountTargetsByARN.Reset()
	b.mtSubnetIdx = make(map[string]map[string]map[string]string)

	for _, mt := range b.mountTargets.All() {
		b.mountTargetsByARN.Put(mt)

		if mt.SubnetID != "" {
			b.mtSubnetStore(mt.region, mt.FileSystemID)[mt.SubnetID] = mt.MountTargetID
		}
	}
}

func (b *InMemoryBackend) rebuildAccessPointIndexes() {
	b.accessPointsByARN.Reset()
	b.accessPointsByClientToken.Reset()
	b.apByFS = make(map[string]map[string]map[string]struct{})

	for _, ap := range b.accessPoints.All() {
		if ap.Tags == nil {
			ap.Tags = tags.New("efs.accesspoint." + ap.AccessPointID + ".tags")
		}

		b.accessPointsByARN.Put(ap)

		if ap.ClientToken != "" {
			b.accessPointsByClientToken.Put(ap)
		}

		b.apFSStore(ap.region, ap.FileSystemID)[ap.AccessPointID] = struct{}{}
	}
}

// Snapshot implements persistence.Persistable by delegating to the backend.
func (h *Handler) Snapshot(ctx context.Context) []byte { return h.Backend.Snapshot(ctx) }

// Restore implements persistence.Persistable by delegating to the backend.
func (h *Handler) Restore(ctx context.Context, data []byte) error {
	return h.Backend.Restore(ctx, data)
}
