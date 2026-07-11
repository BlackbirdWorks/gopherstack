package docdb

// Code in this file supports Phase 3.3 of the datalayer refactor: seven
// resource collections that were previously nested by region (outer key =
// region, e.g. map[string]map[string]*DBCluster) are each replaced by a
// single flat *store.Table[T], keyed by the composite "region|id" string (see
// regionKey in backend.go) -- the region-qualified-table pattern
// services/neptune and services/rds use. Six of the seven also get a
// companion *store.Index grouping entries by region for per-region scans;
// snapshotAttributes is region-qualified too but is only ever looked up or
// written by its exact composite key, never listed by region, so it has no
// byRegion index. GlobalClusters were already flat/partition-scoped (not
// region-nested), so it became a plain (non-composite-key) *store.Table.
//
// tags (map[string]map[string][]Tag) is deliberately NOT converted:
// store.Table requires a *V value with its own identity, but this holds a
// bare []Tag, which carries no key of its own. It remains a plain nested map,
// unchanged by this refactor (see the tagsStore helper at the bottom of
// backend.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func clusterKeyFn(v *DBCluster) string            { return regionKey(v.region, v.DBClusterIdentifier) }
func clusterRegionIndexKeyFn(v *DBCluster) string { return v.region }

func instanceKeyFn(v *DBInstance) string            { return regionKey(v.region, v.DBInstanceIdentifier) }
func instanceRegionIndexKeyFn(v *DBInstance) string { return v.region }

func subnetGroupKeyFn(v *DBSubnetGroup) string            { return regionKey(v.region, v.DBSubnetGroupName) }
func subnetGroupRegionIndexKeyFn(v *DBSubnetGroup) string { return v.region }

func clusterParameterGroupKeyFn(v *DBClusterParameterGroup) string {
	return regionKey(v.region, v.DBClusterParameterGroupName)
}
func clusterParameterGroupRegionIndexKeyFn(v *DBClusterParameterGroup) string { return v.region }

func clusterSnapshotKeyFn(v *DBClusterSnapshot) string {
	return regionKey(v.region, v.DBClusterSnapshotIdentifier)
}
func clusterSnapshotRegionIndexKeyFn(v *DBClusterSnapshot) string { return v.region }

func eventSubscriptionKeyFn(v *EventSubscription) string {
	return regionKey(v.region, v.SubscriptionName)
}
func eventSubscriptionRegionIndexKeyFn(v *EventSubscription) string { return v.region }

// snapshotAttributesKeyFn keys a DBClusterSnapshotAttributesResult the same
// way as the six tables above (composite "region|id"), but this table has no
// companion byRegion index -- see the package doc comment above.
func snapshotAttributesKeyFn(v *DBClusterSnapshotAttributesResult) string {
	return regionKey(v.region, v.DBClusterSnapshotIdentifier)
}

// globalClusterKeyFn keys a GlobalCluster by its own identifier: global
// clusters are partition-scoped, not region-nested, so no composite key is
// needed here (unlike the seven tables above).
func globalClusterKeyFn(v *GlobalCluster) string { return v.GlobalClusterIdentifier }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (and, for the six indexed region-qualified tables, its companion
// store.Table.AddIndex call) to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.clusters = store.Register(b.registry, "clusters", store.New(clusterKeyFn))
		b.clustersByRegion = b.clusters.AddIndex("byRegion", clusterRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.instances = store.Register(b.registry, "instances", store.New(instanceKeyFn))
		b.instancesByRegion = b.instances.AddIndex("byRegion", instanceRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.subnetGroups = store.Register(b.registry, "subnetGroups", store.New(subnetGroupKeyFn))
		b.subnetGroupsByRegion = b.subnetGroups.AddIndex("byRegion", subnetGroupRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.clusterParameterGroups = store.Register(
			b.registry,
			"clusterParameterGroups",
			store.New(clusterParameterGroupKeyFn),
		)
		b.clusterParameterGroupsByRegion = b.clusterParameterGroups.AddIndex(
			"byRegion",
			clusterParameterGroupRegionIndexKeyFn,
		)
	},
	func(b *InMemoryBackend) {
		b.clusterSnapshots = store.Register(b.registry, "clusterSnapshots", store.New(clusterSnapshotKeyFn))
		b.clusterSnapshotsByRegion = b.clusterSnapshots.AddIndex("byRegion", clusterSnapshotRegionIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.eventSubscriptions = store.Register(
			b.registry,
			"eventSubscriptions",
			store.New(eventSubscriptionKeyFn),
		)
		b.eventSubscriptionsByRegion = b.eventSubscriptions.AddIndex(
			"byRegion",
			eventSubscriptionRegionIndexKeyFn,
		)
	},
	func(b *InMemoryBackend) {
		// No AddIndex call: snapshotAttributes is never listed by region, only
		// looked up/written by its exact composite key (see the package doc).
		b.snapshotAttributes = store.Register(
			b.registry,
			"snapshotAttributes",
			store.New(snapshotAttributesKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.globalClusters = store.Register(b.registry, "globalClusters", store.New(globalClusterKeyFn))
	},
}
