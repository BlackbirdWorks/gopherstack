package elasticache

import "github.com/blackbirdworks/gopherstack/pkgs/store"

// Code in this file supports Phase 3.3 of the datalayer refactor: converting
// InMemoryBackend's resource maps to pkgs/store. See pkgs/store's package doc
// and the services/sqs pilot (commit 0f09d77c) / services/ec2 rollout (commit
// 12e611a4) for the pattern this follows.
//
// Every resource in this backend is nested per-region
// (map[string]map[string]*T -- outer key is region) except
// GlobalReplicationGroup, which is partition-scoped like AWS and therefore
// global (see backend.go's InMemoryBackend doc comment). The region-nested
// resources are converted to map[string]*store.Table[T] (region -> Table)
// with a lazy per-region accessor -- see the "*Store" helpers below.
// Because the set of regions is only known at runtime, these per-region
// tables are deliberately NOT registered on a *store.Registry: Registry's
// SnapshotAll/RestoreAll require a fixed, construction-time-known table-name
// set, which a dynamic region set can't provide (a region first seen on
// Restore would have nothing registered to restore into). persistence.go
// instead snapshots/restores each per-region Table directly via
// Table.Snapshot()/Table.Restore(). GlobalReplicationGroup has no such
// per-region dynamism, so it alone is registered on b.registry and goes
// through registry.SnapshotAll()/RestoreAll() in persistence.go.
//
// cacheSecurityGroupIngress is deliberately NOT converted: its value type is
// []EC2SecurityGroupMembership (a slice, not a single *T resource keyed by
// its own identity), which store.Table cannot represent -- it remains a
// plain nested map (map[string]map[string][]EC2SecurityGroupMembership).

func clusterKeyFn(v *Cluster) string                                 { return v.ClusterID }
func replicationGroupKeyFn(v *ReplicationGroup) string               { return v.ReplicationGroupID }
func cacheParameterGroupKeyFn(v *CacheParameterGroup) string         { return v.Name }
func cacheSubnetGroupKeyFn(v *CacheSubnetGroup) string               { return v.Name }
func cacheSnapshotKeyFn(v *CacheSnapshot) string                     { return v.SnapshotName }
func cacheSecurityGroupKeyFn(v *CacheSecurityGroup) string           { return v.Name }
func serverlessCacheKeyFn(v *ServerlessCache) string                 { return v.Name }
func serverlessCacheSnapshotKeyFn(v *ServerlessCacheSnapshot) string { return v.Name }
func userKeyFn(v *User) string                                       { return v.UserID }
func userGroupKeyFn(v *UserGroup) string                             { return v.UserGroupID }
func reservedCacheNodeKeyFn(v *ReservedCacheNode) string             { return v.ReservedCacheNodeID }
func globalReplicationGroupKeyFn(v *GlobalReplicationGroup) string {
	return v.GlobalReplicationGroupID
}

func (b *InMemoryBackend) clustersStore(region string) *store.Table[Cluster] {
	if b.clusters[region] == nil {
		b.clusters[region] = store.New(clusterKeyFn)
	}

	return b.clusters[region]
}

// clustersStoreRO returns the region-scoped clusters table for region
// without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty view instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) clustersStoreRO(region string) *store.Table[Cluster] {
	if v := b.clusters[region]; v != nil {
		return v
	}

	return store.New(clusterKeyFn)
}

func (b *InMemoryBackend) replicationGroupsStore(region string) *store.Table[ReplicationGroup] {
	if b.replicationGroups[region] == nil {
		b.replicationGroups[region] = store.New(replicationGroupKeyFn)
	}

	return b.replicationGroups[region]
}

// replicationGroupsStoreRO returns the region-scoped replicationGroups table
// for region without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty view instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) replicationGroupsStoreRO(region string) *store.Table[ReplicationGroup] {
	if v := b.replicationGroups[region]; v != nil {
		return v
	}

	return store.New(replicationGroupKeyFn)
}

func (b *InMemoryBackend) parameterGroupsStore(region string) *store.Table[CacheParameterGroup] {
	if b.parameterGroups[region] == nil {
		b.initDefaultParameterGroupsForRegion(region)
	}

	return b.parameterGroups[region]
}

// parameterGroupsStoreRO returns the region-scoped parameterGroups table for
// region without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty view instead of lazily seeding the region's default
// parameter groups (and persisting them).
func (b *InMemoryBackend) parameterGroupsStoreRO(region string) *store.Table[CacheParameterGroup] {
	if v := b.parameterGroups[region]; v != nil {
		return v
	}

	return store.New(cacheParameterGroupKeyFn)
}

func (b *InMemoryBackend) subnetGroupsStore(region string) *store.Table[CacheSubnetGroup] {
	if b.subnetGroups[region] == nil {
		b.subnetGroups[region] = store.New(cacheSubnetGroupKeyFn)
	}

	return b.subnetGroups[region]
}

// subnetGroupsStoreRO returns the region-scoped subnetGroups table for
// region without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty view instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) subnetGroupsStoreRO(region string) *store.Table[CacheSubnetGroup] {
	if v := b.subnetGroups[region]; v != nil {
		return v
	}

	return store.New(cacheSubnetGroupKeyFn)
}

func (b *InMemoryBackend) snapshotsStore(region string) *store.Table[CacheSnapshot] {
	if b.snapshots[region] == nil {
		b.snapshots[region] = store.New(cacheSnapshotKeyFn)
	}

	return b.snapshots[region]
}

// snapshotsStoreRO returns the region-scoped snapshots table for region
// without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty view instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) snapshotsStoreRO(region string) *store.Table[CacheSnapshot] {
	if v := b.snapshots[region]; v != nil {
		return v
	}

	return store.New(cacheSnapshotKeyFn)
}

func (b *InMemoryBackend) cacheSecurityGroupsStore(region string) *store.Table[CacheSecurityGroup] {
	if b.cacheSecurityGroups[region] == nil {
		b.cacheSecurityGroups[region] = store.New(cacheSecurityGroupKeyFn)
	}

	return b.cacheSecurityGroups[region]
}

// cacheSecurityGroupsStoreRO returns the region-scoped cacheSecurityGroups
// table for region without mutating the outer map. Safe to call while
// holding only b.mu.RLock(): if the region has not been observed yet, it
// returns a fresh, unregistered, empty view instead of lazily creating (and
// persisting) an entry.
func (b *InMemoryBackend) cacheSecurityGroupsStoreRO(region string) *store.Table[CacheSecurityGroup] {
	if v := b.cacheSecurityGroups[region]; v != nil {
		return v
	}

	return store.New(cacheSecurityGroupKeyFn)
}

func (b *InMemoryBackend) cacheSecurityGroupIngressStore(region string) map[string][]EC2SecurityGroupMembership {
	if b.cacheSecurityGroupIngress[region] == nil {
		b.cacheSecurityGroupIngress[region] = make(map[string][]EC2SecurityGroupMembership)
	}

	return b.cacheSecurityGroupIngress[region]
}

func (b *InMemoryBackend) serverlessCachesStore(region string) *store.Table[ServerlessCache] {
	if b.serverlessCaches[region] == nil {
		b.serverlessCaches[region] = store.New(serverlessCacheKeyFn)
	}

	return b.serverlessCaches[region]
}

// serverlessCachesStoreRO returns the region-scoped serverlessCaches table
// for region without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty view instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) serverlessCachesStoreRO(region string) *store.Table[ServerlessCache] {
	if v := b.serverlessCaches[region]; v != nil {
		return v
	}

	return store.New(serverlessCacheKeyFn)
}

func (b *InMemoryBackend) serverlessCacheSnapshotsStore(region string) *store.Table[ServerlessCacheSnapshot] {
	if b.serverlessCacheSnapshots[region] == nil {
		b.serverlessCacheSnapshots[region] = store.New(serverlessCacheSnapshotKeyFn)
	}

	return b.serverlessCacheSnapshots[region]
}

// serverlessCacheSnapshotsStoreRO returns the region-scoped
// serverlessCacheSnapshots table for region without mutating the outer map.
// Safe to call while holding only b.mu.RLock(): if the region has not been
// observed yet, it returns a fresh, unregistered, empty view instead of
// lazily creating (and persisting) an entry.
func (b *InMemoryBackend) serverlessCacheSnapshotsStoreRO(region string) *store.Table[ServerlessCacheSnapshot] {
	if v := b.serverlessCacheSnapshots[region]; v != nil {
		return v
	}

	return store.New(serverlessCacheSnapshotKeyFn)
}

func (b *InMemoryBackend) usersStore(region string) *store.Table[User] {
	if b.users[region] == nil {
		b.users[region] = store.New(userKeyFn)
	}

	return b.users[region]
}

// usersStoreRO returns the region-scoped users table for region without
// mutating the outer map. Safe to call while holding only b.mu.RLock(): if
// the region has not been observed yet, it returns a fresh, unregistered,
// empty view instead of lazily creating (and persisting) an entry.
func (b *InMemoryBackend) usersStoreRO(region string) *store.Table[User] {
	if v := b.users[region]; v != nil {
		return v
	}

	return store.New(userKeyFn)
}

func (b *InMemoryBackend) userGroupsStore(region string) *store.Table[UserGroup] {
	if b.userGroups[region] == nil {
		b.userGroups[region] = store.New(userGroupKeyFn)
	}

	return b.userGroups[region]
}

// userGroupsStoreRO returns the region-scoped userGroups table for region
// without mutating the outer map. Safe to call while holding only
// b.mu.RLock(): if the region has not been observed yet, it returns a fresh,
// unregistered, empty view instead of lazily creating (and persisting) an
// entry.
func (b *InMemoryBackend) userGroupsStoreRO(region string) *store.Table[UserGroup] {
	if v := b.userGroups[region]; v != nil {
		return v
	}

	return store.New(userGroupKeyFn)
}

func (b *InMemoryBackend) reservedCacheNodesStore(region string) *store.Table[ReservedCacheNode] {
	if b.reservedCacheNodes[region] == nil {
		b.reservedCacheNodes[region] = store.New(reservedCacheNodeKeyFn)
	}

	return b.reservedCacheNodes[region]
}

// reservedCacheNodesStoreRO returns the region-scoped reservedCacheNodes
// table for region without mutating the outer map. Safe to call while
// holding only b.mu.RLock(): if the region has not been observed yet, it
// returns a fresh, unregistered, empty view instead of lazily creating (and
// persisting) an entry.
func (b *InMemoryBackend) reservedCacheNodesStoreRO(region string) *store.Table[ReservedCacheNode] {
	if v := b.reservedCacheNodes[region]; v != nil {
		return v
	}

	return store.New(reservedCacheNodeKeyFn)
}
