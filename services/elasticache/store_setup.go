package elasticache

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
// with a lazy per-region accessor -- see the "*Store" helpers in backend.go.
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
