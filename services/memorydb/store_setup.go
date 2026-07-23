package memorydb

import "github.com/blackbirdworks/gopherstack/pkgs/store"

// Code in this file supports Phase 3.3 of the datalayer refactor: converting
// InMemoryBackend's resource maps to pkgs/store. See pkgs/store's package doc
// and the services/sqs pilot (commit 0f09d77c), services/ec2 rollout (commit
// 12e611a4), and services/elasticache rollout (commit 06806317) for the
// pattern this follows.
//
// Every resource in this backend is nested per-region
// (map[string]map[string]*T -- outer key is region) except
// MultiRegionCluster, MultiRegionParameterGroup, and ServiceUpdate, which are
// partition-scoped like AWS and therefore global (see backend.go's
// InMemoryBackend doc comment). The region-nested resources are converted to
// map[string]*store.Table[T] (region -> Table) with a lazy per-region
// accessor -- see the "*Store" helpers in backend.go. Because the set of
// regions is only known at runtime, these per-region tables are deliberately
// NOT registered on a *store.Registry: Registry's SnapshotAll/RestoreAll
// require a fixed, construction-time-known table-name set, which a dynamic
// region set can't provide (a region first seen on Restore would have
// nothing registered to restore into). persistence.go instead
// snapshots/restores each per-region Table directly via
// Table.Snapshot()/Table.Restore(). The three global resources have no such
// per-region dynamism, so they alone are registered on b.registry and go
// through registry.SnapshotAll()/RestoreAll() in persistence.go.
//
// arnToResource and events are deliberately NOT converted: arnToResource's
// value type (resourceRef) has no field carrying its own key (the ARN is the
// map key, not a value field) so it cannot supply a store.Table keyFn, and
// events is slice-valued (map[string][]*Event) rather than a single *T
// resource keyed by its own identity, which store.Table cannot represent.
// Both remain plain nested maps.

func clusterKeyFn(v *Cluster) string               { return v.Name }
func aclKeyFn(v *ACL) string                       { return v.Name }
func subnetGroupKeyFn(v *SubnetGroup) string       { return v.Name }
func userKeyFn(v *User) string                     { return v.Name }
func parameterGroupKeyFn(v *ParameterGroup) string { return v.Name }
func snapshotKeyFn(v *Snapshot) string             { return v.Name }
func reservedNodeKeyFn(v *ReservedNode) string     { return v.ReservationID }

func multiRegionClusterKeyFn(v *MultiRegionCluster) string               { return v.MultiRegionClusterName }
func multiRegionParameterGroupKeyFn(v *MultiRegionParameterGroup) string { return v.Name }
func serviceUpdateKeyFn(v *ServiceUpdate) string                         { return v.ServiceUpdateName }

// tableGet and tableAll wrap the corresponding *store.Table[V]
// methods but tolerate a nil t, returning the same zero values a lookup
// against a nil map would have returned pre-conversion. This matters because
// several read paths in backend.go deliberately read the raw
// map[string]*store.Table[V] field directly (e.g. b.clusters[region])
// instead of going through the lazy "*Store" accessor, so as to not allocate
// an entry for a region that has never been written to; unlike a nil map, a
// nil *store.Table[V] cannot be called directly without panicking.
func tableGet[V any](t *store.Table[V], id string) (*V, bool) {
	if t == nil {
		return nil, false
	}

	return t.Get(id)
}

func tableAll[V any](t *store.Table[V]) []*V {
	if t == nil {
		return nil
	}

	return t.All()
}
