package elasticache

import (
	"context"
	"encoding/json"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// elasticacheSnapshotVersion identifies the shape of [backendSnapshot]. It
// must be bumped whenever a change to clusterSnapshot, or to the set/shape of
// tables captured below, would make an older snapshot unsafe to decode as the
// current shape. Restore compares this against the persisted value and
// discards (rather than attempts to partially decode) any mismatch -- see
// Restore below. This mirrors the services/sqs pilot (commit 0f09d77c).
const elasticacheSnapshotVersion = 1

// clusterSnapshot captures the serialisable fields of a Cluster: it omits the
// unexported live miniredis instance (which can never round-trip through
// JSON) and mirrors the field subset this backend has always persisted for
// clusters. Cluster carries several additional exported fields (e.g.
// ReplicationGroupID, AvailableAt, Members) that predate this conversion and
// were never included in the persisted shape; that pre-existing behavior is
// preserved as-is here rather than "fixed" as part of this mechanical
// datalayer swap.
type clusterSnapshot struct {
	CreatedAt                  time.Time  `json:"createdAt"`
	Tags                       *tags.Tags `json:"tags,omitempty"`
	ClusterID                  string     `json:"clusterID"`
	Engine                     string     `json:"engine"`
	EngineVersion              string     `json:"engineVersion"`
	Status                     string     `json:"status"`
	Endpoint                   string     `json:"endpoint"`
	NodeType                   string     `json:"nodeType"`
	ARN                        string     `json:"arn"`
	CacheParameterGroupName    string     `json:"cacheParameterGroupName,omitempty"`
	PreferredMaintenanceWindow string     `json:"preferredMaintenanceWindow,omitempty"`
	SnapshotWindow             string     `json:"snapshotWindow,omitempty"`
	Port                       int        `json:"port"`
	NumCacheNodes              int        `json:"numCacheNodes"`
}

// backendSnapshot is the top-level on-disk shape for the ElastiCache backend.
//
// Tables holds the JSON-encoded array for every table registered on
// b.registry (currently just "globalReplicationGroups", the one resource that
// is global rather than per-region -- see store_setup.go), produced by
// [store.Registry.SnapshotAll].
//
// Every other resource is nested per-region (region -> *store.Table[T]) and
// is NOT registered on b.registry, because the set of regions is only known
// at runtime (see store_setup.go's doc comment); each is instead captured
// directly below as region -> that table's own deterministic
// [store.Table.Snapshot] slice.
type backendSnapshot struct {
	Tables                    map[string]json.RawMessage                         `json:"tables"`
	Clusters                  map[string][]*clusterSnapshot                      `json:"clusters"`
	ReplicationGroups         map[string][]*ReplicationGroup                     `json:"replicationGroups"`
	ParameterGroups           map[string][]*CacheParameterGroup                  `json:"parameterGroups"`
	SubnetGroups              map[string][]*CacheSubnetGroup                     `json:"subnetGroups"`
	Snapshots                 map[string][]*CacheSnapshot                        `json:"snapshots"`
	CacheSecurityGroups       map[string][]*CacheSecurityGroup                   `json:"cacheSecurityGroups,omitempty"`
	CacheSecurityGroupIngress map[string]map[string][]EC2SecurityGroupMembership `json:"cacheSecurityGroupIngress,omitempty"` //nolint:lll // struct tag cannot be split
	ServerlessCaches          map[string][]*ServerlessCache                      `json:"serverlessCaches,omitempty"`
	ServerlessCacheSnapshots  map[string][]*ServerlessCacheSnapshot              `json:"serverlessCacheSnapshots,omitempty"` //nolint:lll // struct tag cannot be split
	Users                     map[string][]*User                                 `json:"users,omitempty"`
	UserGroups                map[string][]*UserGroup                            `json:"userGroups,omitempty"`
	ReservedCacheNodes        map[string][]*ReservedCacheNode                    `json:"reservedCacheNodes,omitempty"`
	EngineMode                string                                             `json:"engineMode"`
	AccountID                 string                                             `json:"accountID"`
	Region                    string                                             `json:"region"`
	Events                    []CacheEvent                                       `json:"events,omitempty"`
	Version                   int                                                `json:"version"`
}

// snapshotAllRegions captures the deterministic [store.Table.Snapshot] slice
// for every region in m.
func snapshotAllRegions[T any](m map[string]*store.Table[T]) map[string][]*T {
	out := make(map[string][]*T, len(m))
	for region, t := range m {
		out[region] = t.Snapshot()
	}

	return out
}

// Snapshot serialises the backend state to JSON.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Snapshot(ctx context.Context) []byte {
	b.mu.RLock("Snapshot")
	defer b.mu.RUnlock()

	tables, err := b.registry.SnapshotAll()
	if err != nil {
		// The registered tables are plain JSON-friendly structs, so a marshal
		// failure here would indicate a programming error rather than bad
		// input data. Log and skip the snapshot rather than panic, matching
		// the persistence.Persistable contract (nil is skipped by the Manager).
		logger.Load(ctx).WarnContext(ctx, "elasticache: snapshot table marshal failed", "error", err)

		return nil
	}

	clusters := make(map[string][]*clusterSnapshot, len(b.clusters))
	for region, t := range b.clusters {
		items := t.Snapshot()
		snaps := make([]*clusterSnapshot, len(items))

		for i, c := range items {
			snaps[i] = &clusterSnapshot{
				CreatedAt:                  c.CreatedAt,
				Tags:                       c.Tags,
				ClusterID:                  c.ClusterID,
				Engine:                     c.Engine,
				EngineVersion:              c.EngineVersion,
				Status:                     c.Status,
				Endpoint:                   c.Endpoint,
				NodeType:                   c.NodeType,
				ARN:                        c.ARN,
				CacheParameterGroupName:    c.CacheParameterGroupName,
				PreferredMaintenanceWindow: c.PreferredMaintenanceWindow,
				SnapshotWindow:             c.SnapshotWindow,
				Port:                       c.Port,
				NumCacheNodes:              c.NumCacheNodes,
			}
		}

		clusters[region] = snaps
	}

	snap := backendSnapshot{
		Version:                   elasticacheSnapshotVersion,
		Tables:                    tables,
		Clusters:                  clusters,
		ReplicationGroups:         snapshotAllRegions(b.replicationGroups),
		ParameterGroups:           snapshotAllRegions(b.parameterGroups),
		SubnetGroups:              snapshotAllRegions(b.subnetGroups),
		Snapshots:                 snapshotAllRegions(b.snapshots),
		CacheSecurityGroups:       snapshotAllRegions(b.cacheSecurityGroups),
		CacheSecurityGroupIngress: b.cacheSecurityGroupIngress,
		ServerlessCaches:          snapshotAllRegions(b.serverlessCaches),
		ServerlessCacheSnapshots:  snapshotAllRegions(b.serverlessCacheSnapshots),
		Users:                     snapshotAllRegions(b.users),
		UserGroups:                snapshotAllRegions(b.userGroups),
		ReservedCacheNodes:        snapshotAllRegions(b.reservedCacheNodes),
		Events:                    b.events.marshalJSON(),
		EngineMode:                b.engineMode,
		AccountID:                 b.accountID,
		Region:                    b.region,
	}

	return persistence.MarshalSnapshot(ctx, "elasticache", snap)
}

// restoreRegionTables resets the outer per-region map via reset, then
// restores each region's table from data using storeFn (one of the
// InMemoryBackend "*Store" lazy accessors). This mirrors the pre-conversion
// full-replace Restore semantics: a region present in the live backend but
// absent from data ends up empty, exactly as a direct `b.field = data`
// assignment would have left it.
func restoreRegionTables[T any](reset func(), storeFn func(string) *store.Table[T], data map[string][]*T) {
	reset()

	for region, items := range data {
		storeFn(region).Restore(items)
	}
}

// restoreClusterRegion converts one region's persisted clusterSnapshot slice
// into live *Cluster values (mini stays nil; a restored cluster has no live
// data-plane engine until re-created, matching pre-conversion behavior).
func restoreClusterRegion(items []*clusterSnapshot) []*Cluster {
	out := make([]*Cluster, len(items))
	for i, cs := range items {
		out[i] = &Cluster{
			CreatedAt:                  cs.CreatedAt,
			Tags:                       cs.Tags,
			ClusterID:                  cs.ClusterID,
			Engine:                     cs.Engine,
			EngineVersion:              cs.EngineVersion,
			Status:                     cs.Status,
			Endpoint:                   cs.Endpoint,
			NodeType:                   cs.NodeType,
			ARN:                        cs.ARN,
			CacheParameterGroupName:    cs.CacheParameterGroupName,
			PreferredMaintenanceWindow: cs.PreferredMaintenanceWindow,
			SnapshotWindow:             cs.SnapshotWindow,
			Port:                       cs.Port,
			NumCacheNodes:              cs.NumCacheNodes,
		}
	}

	return out
}

// Restore loads backend state from a JSON snapshot.
// It implements persistence.Persistable.
func (b *InMemoryBackend) Restore(ctx context.Context, data []byte) error {
	var snap backendSnapshot

	if err := persistence.UnmarshalSnapshot(ctx, "elasticache", data, &snap); err != nil {
		return err
	}

	b.mu.Lock("Restore")
	defer b.mu.Unlock()

	if snap.Version != elasticacheSnapshotVersion {
		// An incompatible (older/newer/absent) snapshot version must never be
		// partially decoded as the current shape -- that risks silently
		// misinterpreting fields. Discard cleanly and start empty instead of
		// erroring, since this is an expected, recoverable condition (e.g.
		// upgrading gopherstack across a snapshot-format change), not data
		// corruption.
		logger.Load(ctx).WarnContext(ctx,
			"elasticache: discarding incompatible snapshot version, starting empty",
			"gotVersion", snap.Version, "wantVersion", elasticacheSnapshotVersion)

		b.resetLocked()

		return nil
	}

	if err := b.registry.RestoreAll(snap.Tables); err != nil {
		return err
	}

	b.closeLiveClusterEnginesLocked()

	b.clusters = make(map[string]*store.Table[Cluster])
	for region, items := range snap.Clusters {
		b.clustersStore(region).Restore(restoreClusterRegion(items))
	}

	restoreRegionTables(func() { b.replicationGroups = make(map[string]*store.Table[ReplicationGroup]) },
		b.replicationGroupsStore, snap.ReplicationGroups)
	restoreRegionTables(func() { b.parameterGroups = make(map[string]*store.Table[CacheParameterGroup]) },
		b.parameterGroupsStore, snap.ParameterGroups)
	restoreRegionTables(func() { b.subnetGroups = make(map[string]*store.Table[CacheSubnetGroup]) },
		b.subnetGroupsStore, snap.SubnetGroups)
	restoreRegionTables(func() { b.snapshots = make(map[string]*store.Table[CacheSnapshot]) },
		b.snapshotsStore, snap.Snapshots)
	restoreRegionTables(func() { b.cacheSecurityGroups = make(map[string]*store.Table[CacheSecurityGroup]) },
		b.cacheSecurityGroupsStore, snap.CacheSecurityGroups)
	restoreRegionTables(func() { b.serverlessCaches = make(map[string]*store.Table[ServerlessCache]) },
		b.serverlessCachesStore, snap.ServerlessCaches)
	restoreRegionTables(
		func() { b.serverlessCacheSnapshots = make(map[string]*store.Table[ServerlessCacheSnapshot]) },
		b.serverlessCacheSnapshotsStore, snap.ServerlessCacheSnapshots,
	)
	restoreRegionTables(func() { b.users = make(map[string]*store.Table[User]) },
		b.usersStore, snap.Users)
	restoreRegionTables(func() { b.userGroups = make(map[string]*store.Table[UserGroup]) },
		b.userGroupsStore, snap.UserGroups)
	restoreRegionTables(func() { b.reservedCacheNodes = make(map[string]*store.Table[ReservedCacheNode]) },
		b.reservedCacheNodesStore, snap.ReservedCacheNodes)

	if snap.CacheSecurityGroupIngress != nil {
		b.cacheSecurityGroupIngress = snap.CacheSecurityGroupIngress
	} else {
		b.cacheSecurityGroupIngress = make(map[string]map[string][]EC2SecurityGroupMembership)
	}

	b.events.restoreFromSlice(snap.Events)

	b.engineMode = snap.EngineMode
	b.accountID = snap.AccountID
	b.region = snap.Region

	b.reinitMissingDefaultParameterGroupsLocked()

	return nil
}

// closeLiveClusterEnginesLocked closes every live miniredis instance across
// all regions before the cluster tables are discarded and rebuilt from a
// snapshot. Must hold b.mu.
func (b *InMemoryBackend) closeLiveClusterEnginesLocked() {
	for _, regionClusters := range b.clusters {
		for _, c := range regionClusters.All() {
			if c.mini != nil {
				c.mini.Close()
			}
		}
	}
}

// reinitMissingDefaultParameterGroupsLocked re-seeds the well-known default
// parameter groups for the backend's current region if a restored snapshot
// didn't already include them (e.g. an older snapshot). Must hold b.mu.
func (b *InMemoryBackend) reinitMissingDefaultParameterGroupsLocked() {
	regionStore := b.parameterGroupsStore(b.region)

	for _, dpg := range builtinParameterGroupFamilies() {
		if _, ok := regionStore.Get(dpg.name); ok {
			continue
		}

		regionStore.Put(&CacheParameterGroup{
			Name:        dpg.name,
			Family:      dpg.family,
			Description: "Default parameter group for " + dpg.family,
			ARN:         b.parameterGroupARN(dpg.name),
			IsGlobal:    true,
			Parameters:  make(map[string]string),
			Tags:        tags.New("elasticache.pg." + dpg.name + ".tags"),
		})
	}
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
		return r.Restore(ctx, data)
	}

	return nil
}
