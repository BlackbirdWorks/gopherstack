package dax

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/codebuild (data-driven, direct registration -- see pkgs/store's
// package doc for the underlying primitive).
//
// clusters, paramGroups, and subnetGroups each key off a real, non-json:"-"
// identity field the value type already carries (Cluster.ClusterName,
// ParameterGroup.ParameterGroupName, SubnetGroup.SubnetGroupName), so all
// three register directly on b.registry -- no DTO indirection and no
// secondary store.Index are needed: DAX has no ARN-keyed reverse-lookup map
// (arnExists/clusterByARN parse the resource name back out of the ARN string
// and look it up directly) and no cross-cluster grouping to derive.
//
// tags (map[string]map[string]string, keyed by ARN rather than *T) and events
// ([]*Event, an ordered ring buffer, not a keyed collection) are left as
// plain fields -- neither fits store.Table's keyed-collection shape. See
// persistence.go for how they round-trip alongside the registered tables.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func clusterKeyFn(v *Cluster) string { return v.ClusterName }

func paramGroupKeyFn(v *ParameterGroup) string { return v.ParameterGroupName }

func subnetGroupKeyFn(v *SubnetGroup) string { return v.SubnetGroupName }

// registerAllTables registers every backend resource table exactly once. It
// must be called during construction only, immediately after b.registry is
// created -- store.Register panics on a duplicate name, so runtime resets go
// through b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.clusters = store.Register(b.registry, "clusters", store.New(clusterKeyFn))
	b.paramGroups = store.Register(b.registry, "paramGroups", store.New(paramGroupKeyFn))
	b.subnetGroups = store.Register(b.registry, "subnetGroups", store.New(subnetGroupKeyFn))
}
