package resourcegroups

// Code in this file supports Phase 3.3 of the datalayer refactor: the two
// region-nested resource collections (previously map[region]map[key]*T, with
// groups additionally carrying a companion map[region]map[ARN]name reverse
// index) are replaced by a flat *store.Table[T] keyed by the composite
// "region|id" string (see regionKey in store.go), with companion
// *store.Index values: one grouping entries by region (for per-region list
// scans, replacing the old outer map nesting) and, for groups, one grouping
// entries by "region|ARN" (replacing the old per-region ARN reverse map used
// by GetTagsByARN/AddTagsByARN/RemoveTagsByARN). It otherwise follows
// pkgs/store's package doc and the services/scheduler (region-qualified-table)
// conversion.
//
// Neither Group nor TagSyncTask carries an exported Region field (not part of
// either's AWS wire shape) -- each always embeds the region it was created in
// within its own ARN (see CreateGroup/StartTagSyncTask), so groupRegionOf/
// taskRegionOf (store.go) derive it from there instead of adding a new
// field.
//
// groups is a "dirty" table in the persistence sense: each Group carries a
// live *tags.Tags field (a Prometheus-instrumented map) that plain
// json.Marshal cannot round-trip while preserving the per-resource metric
// name -- so it is NOT registered on the shared b.registry; it is built with
// store.New only, and persistence.go handles its Snapshot/Restore via a
// plain-map DTO (see that file). tagSyncTasks has no such field (its only
// non-primitive field, ResourceQuery, and its CreatedAt time.Time both
// round-trip cleanly through plain json.Marshal), so it is registered
// directly on b.registry.
//
// groupConfigurations, groupResources, and groupingStatuses remain plain
// region-nested maps and are left entirely unconverted: their values (a slice
// of configuration items, a slice of resource ARN strings, a slice of
// grouping-status records) have no identity of their own for store.Table to
// key on.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// registerAllTables builds the backend's two resource tables and their
// indexes exactly once, during construction. tagSyncTasks is registered on
// b.registry (clean); groups is NOT (dirty; see the file doc) -- runtime
// resets for it go through its own Table.Reset() (see InMemoryBackend.Reset
// in store.go) rather than b.registry.ResetAll().
func registerAllTables(b *InMemoryBackend) {
	b.tagSyncTasks = store.Register(b.registry, "tagSyncTasks", store.New(b.tagSyncTaskTableKeyFn))
	b.tagSyncTasksByRegion = b.tagSyncTasks.AddIndex("byRegion", b.tagSyncTaskRegionIndexKeyFn)

	b.groups = store.New(b.groupTableKeyFn)
	b.groupsByRegion = b.groups.AddIndex("byRegion", b.groupRegionIndexKeyFn)
	b.groupsByARN = b.groups.AddIndex("byARN", b.groupARNIndexKeyFn)
}
