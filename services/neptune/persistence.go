package neptune

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// neptuneSnapshotVersion identifies the shape of [backendSnapshot]. It must be
// bumped whenever a change to regionalDTO/backendSnapshot itself would make an
// older snapshot unsafe to decode as the current shape (e.g. a field's
// meaning or type changes). Restore compares this against the persisted value
// and discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. This is the first version: earlier (pre-Phase-3.3) snapshots
// carried no version field at all, so they also fail this check and are
// discarded rather than misread, which is the safe behaviour across any
// snapshot-format change.
const neptuneSnapshotVersion = 1

// regionalDTO wraps a region-nested resource for JSON round-tripping through
// store.Registry. Table[V].Snapshot's plain json.Marshal(V) cannot see the
// unexported `region` field each of DBCluster/DBInstance/DBSubnetGroup/
// DBClusterParameterGroup/DBClusterSnapshot/DBParameterGroup/
// DBClusterEndpoint/EventSubscription carries (used only for the live table's
// composite key -- see store_setup.go), so it is carried alongside Value here
// instead. ID mirrors the resource's own identifier so the DTO table itself
// has a stable composite key ("Region|ID") independent of which concrete V it
// wraps, without needing a per-type identifier accessor.
type regionalDTO[V any] struct {
	Value  *V     `json:"value"`
	Region string `json:"region"`
	ID     string `json:"id"`
}

// regionalDTOKeyFn is the shared [store.Table] key function for every
// regionalDTO[V] table below; it mirrors the "region|id" composite key each
// live table uses (see regionKey in backend.go).
func regionalDTOKeyFn[V any](d *regionalDTO[V]) string { return regionKey(d.Region, d.ID) }

// backendSnapshot is the top-level on-disk shape for the Neptune backend.
//
// Tables holds one JSON-encoded array per registered DTO table, produced by
// [store.Registry.SnapshotAll] -- the eight region-qualified resource tables
// (each wrapped in a regionalDTO to carry its region field through) plus
// globalClusters (registered directly: GlobalCluster has no hidden field).
// ClusterRoles and Tags are still region-nested plain maps (see
// store_setup.go for why they were not converted to store.Table) and are
// persisted directly, as before. Version guards against decoding a snapshot
// from an incompatible (older or newer) build of this backend as though it
// were the current shape; see Restore.
type backendSnapshot struct {
	Tables       map[string]json.RawMessage     `json:"tables"`
	ClusterRoles map[string]map[string][]string `json:"clusterRoles"`
	Tags         map[string]map[string][]Tag    `json:"tags"`
	AccountID    string                         `json:"accountID"`
	Region       string                         `json:"region"`
	Version      int                            `json:"version"`
}

// buildPersistenceDTORegistry constructs the ephemeral DTO registry used by
// both Snapshot and Restore. It is built fresh on every call (rather than
// reusing b.registry) because the DTO value types differ from the live table
// value types (regionalDTO[V] vs V for the eight region-qualified tables).
func buildPersistenceDTORegistry() (
	*store.Registry,
	*store.Table[regionalDTO[DBCluster]],
	*store.Table[regionalDTO[DBInstance]],
	*store.Table[regionalDTO[DBSubnetGroup]],
	*store.Table[regionalDTO[DBClusterParameterGroup]],
	*store.Table[regionalDTO[DBClusterSnapshot]],
	*store.Table[regionalDTO[DBParameterGroup]],
	*store.Table[regionalDTO[DBClusterEndpoint]],
	*store.Table[regionalDTO[EventSubscription]],
	*store.Table[GlobalCluster],
) {
	dtoReg := store.NewRegistry()
	clusterDTOs := store.Register(dtoReg, "clusters", store.New(regionalDTOKeyFn[DBCluster]))
	instanceDTOs := store.Register(dtoReg, "instances", store.New(regionalDTOKeyFn[DBInstance]))
	subnetGroupDTOs := store.Register(dtoReg, "subnetGroups", store.New(regionalDTOKeyFn[DBSubnetGroup]))
	clusterParameterGroupDTOs := store.Register(
		dtoReg, "clusterParameterGroups", store.New(regionalDTOKeyFn[DBClusterParameterGroup]),
	)
	clusterSnapshotDTOs := store.Register(
		dtoReg, "clusterSnapshots", store.New(regionalDTOKeyFn[DBClusterSnapshot]),
	)
	parameterGroupDTOs := store.Register(
		dtoReg, "parameterGroups", store.New(regionalDTOKeyFn[DBParameterGroup]),
	)
	clusterEndpointDTOs := store.Register(
		dtoReg, "clusterEndpoints", store.New(regionalDTOKeyFn[DBClusterEndpoint]),
	)
	eventSubscriptionDTOs := store.Register(
		dtoReg, "eventSubscriptions", store.New(regionalDTOKeyFn[EventSubscription]),
	)
	globalClusterDTOs := store.Register(dtoReg, "globalClusters", store.New(globalClusterKeyFn))

	return dtoReg, clusterDTOs, instanceDTOs, subnetGroupDTOs, clusterParameterGroupDTOs,
		clusterSnapshotDTOs, parameterGroupDTOs, clusterEndpointDTOs, eventSubscriptionDTOs, globalClusterDTOs
}

// Snapshot serialises the backend state to JSON. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	dtoReg, clusterDTOs, instanceDTOs, subnetGroupDTOs, clusterParameterGroupDTOs,
		clusterSnapshotDTOs, parameterGroupDTOs, clusterEndpointDTOs, eventSubscriptionDTOs,
		globalClusterDTOs := buildPersistenceDTORegistry()

	for _, v := range b.clusters.Snapshot() {
		clusterDTOs.Put(&regionalDTO[DBCluster]{Region: v.region, ID: v.DBClusterIdentifier, Value: v})
	}
	for _, v := range b.instances.Snapshot() {
		instanceDTOs.Put(&regionalDTO[DBInstance]{Region: v.region, ID: v.DBInstanceIdentifier, Value: v})
	}
	for _, v := range b.subnetGroups.Snapshot() {
		subnetGroupDTOs.Put(&regionalDTO[DBSubnetGroup]{Region: v.region, ID: v.DBSubnetGroupName, Value: v})
	}
	for _, v := range b.clusterParameterGroups.Snapshot() {
		clusterParameterGroupDTOs.Put(&regionalDTO[DBClusterParameterGroup]{
			Region: v.region, ID: v.DBClusterParameterGroupName, Value: v,
		})
	}
	for _, v := range b.clusterSnapshots.Snapshot() {
		clusterSnapshotDTOs.Put(&regionalDTO[DBClusterSnapshot]{
			Region: v.region, ID: v.DBClusterSnapshotIdentifier, Value: v,
		})
	}
	for _, v := range b.parameterGroups.Snapshot() {
		parameterGroupDTOs.Put(&regionalDTO[DBParameterGroup]{Region: v.region, ID: v.DBParameterGroupName, Value: v})
	}
	for _, v := range b.clusterEndpoints.Snapshot() {
		clusterEndpointDTOs.Put(&regionalDTO[DBClusterEndpoint]{
			Region: v.region, ID: v.DBClusterEndpointIdentifier, Value: v,
		})
	}
	for _, v := range b.eventSubscriptions.Snapshot() {
		eventSubscriptionDTOs.Put(&regionalDTO[EventSubscription]{
			Region: v.region, ID: v.CustSubscriptionID, Value: v,
		})
	}
	for _, v := range b.globalClusters.Snapshot() {
		globalClusterDTOs.Put(v)
	}

	tables, err := dtoReg.SnapshotAll()
	if err != nil {
		// The DTOs above are plain JSON-friendly structs, so a marshal failure
		// here would indicate a programming error rather than bad input data.
		// Log and skip the snapshot rather than panic, matching the
		// persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx,
			"neptune: snapshot table marshal failed", "error", err)

		return nil
	}

	snap := backendSnapshot{
		Version:      neptuneSnapshotVersion,
		Tables:       tables,
		ClusterRoles: b.clusterRoles,
		Tags:         b.tags,
		AccountID:    b.accountID,
		Region:       b.region,
	}

	return persistence.MarshalSnapshot(ctx, "neptune", snap)
}

// Restore loads backend state from a JSON snapshot. It implements
// persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "neptune", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != neptuneSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"neptune: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", neptuneSnapshotVersion)

		b.registry.ResetAll()
		b.clusterRoles = make(map[string]map[string][]string)
		b.tags = make(map[string]map[string][]Tag)
		b.accountID = snap.AccountID
		b.region = snap.Region

		return nil
	}

	if err := b.restoreResourceTables(snap.Tables); err != nil {
		return err
	}

	if snap.ClusterRoles == nil {
		snap.ClusterRoles = make(map[string]map[string][]string)
	}
	b.clusterRoles = snap.ClusterRoles

	if snap.Tags == nil {
		snap.Tags = make(map[string]map[string][]Tag)
	}
	b.tags = snap.Tags

	b.accountID = snap.AccountID
	b.region = snap.Region

	return nil
}

// unwrapRegionalDTOs converts every regionalDTO[V] in dtos into its live *V,
// restoring the unexported region field each carries via setRegion (a plain
// generic type parameter V has no field access, so the region assignment --
// e.g. `func(v *DBCluster, r string) { v.region = r }` -- is supplied by the
// caller, one per concrete V; see restoreResourceTables). A DTO whose Value is
// nil -- not producible by Snapshot, but a defensive guard against a
// hand-edited or corrupted snapshot -- is skipped rather than dereferenced.
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

// restoreResourceTables rebuilds every store.Table on b from snap's
// per-table JSON, factored out of Restore to keep Restore's own cognitive
// complexity low. Callers must hold b.mu.Lock.
func (b *InMemoryBackend) restoreResourceTables(tables map[string]json.RawMessage) error {
	dtoReg, clusterDTOs, instanceDTOs, subnetGroupDTOs, clusterParameterGroupDTOs,
		clusterSnapshotDTOs, parameterGroupDTOs, clusterEndpointDTOs, eventSubscriptionDTOs,
		globalClusterDTOs := buildPersistenceDTORegistry()

	if err := dtoReg.RestoreAll(tables); err != nil {
		return fmt.Errorf("neptune: restore snapshot tables: %w", err)
	}

	b.clusters.Restore(unwrapRegionalDTOs(clusterDTOs, func(v *DBCluster, r string) { v.region = r }))
	b.instances.Restore(unwrapRegionalDTOs(instanceDTOs, func(v *DBInstance, r string) { v.region = r }))
	b.subnetGroups.Restore(unwrapRegionalDTOs(subnetGroupDTOs, func(v *DBSubnetGroup, r string) { v.region = r }))
	b.clusterParameterGroups.Restore(unwrapRegionalDTOs(
		clusterParameterGroupDTOs, func(v *DBClusterParameterGroup, r string) { v.region = r },
	))
	b.clusterSnapshots.Restore(unwrapRegionalDTOs(
		clusterSnapshotDTOs, func(v *DBClusterSnapshot, r string) { v.region = r },
	))
	b.parameterGroups.Restore(unwrapRegionalDTOs(
		parameterGroupDTOs, func(v *DBParameterGroup, r string) { v.region = r },
	))
	b.clusterEndpoints.Restore(unwrapRegionalDTOs(
		clusterEndpointDTOs, func(v *DBClusterEndpoint, r string) { v.region = r },
	))
	b.eventSubscriptions.Restore(unwrapRegionalDTOs(
		eventSubscriptionDTOs, func(v *EventSubscription, r string) { v.region = r },
	))
	b.globalClusters.Restore(globalClusterDTOs.Snapshot())

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
