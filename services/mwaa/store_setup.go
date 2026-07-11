package mwaa

// Code in this file supports Phase 3.3 of the datalayer refactor: the single
// region-nested resource collection (previously map[string]map[string]*Environment,
// outer key = region) is replaced by a flat *store.Table[Environment], keyed
// by the composite "region|name" string (see regionKey in backend.go), with
// two companion *store.Index values: one grouping entries by region (for
// per-region list operations, replacing the old outer map nesting) and one
// grouping entries by "region|ARN" (replacing the old per-region ARN reverse
// map, region-scoped to preserve the exact isolation the old nested map gave
// -- see TestMWAATagsAndMetricsRegionIsolation). It otherwise follows
// pkgs/store's package doc and the services/neptune (region-qualified-table)
// conversion.
//
// metrics (map[string]map[string][]MetricDatum) is deliberately NOT
// converted: store.Table requires a *V value with its own identity, but each
// entry here is a bare []MetricDatum slice with no identifier of its own. It
// remains a plain nested map, unchanged by this refactor.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// environmentKeyFn derives the composite primary key for the flat
// environments table from the region-qualifying region field and the
// environment's own Name.
func environmentKeyFn(v *Environment) string { return regionKey(v.region, v.Name) }

// environmentRegionIndexKeyFn groups environments by region alone, replacing
// the old outer map[region] nesting for per-region scans (ListEnvironmentsPage).
func environmentRegionIndexKeyFn(v *Environment) string { return v.region }

// environmentARNIndexKeyFn groups environments by "region|ARN", replacing the
// old per-region ARN reverse map (region → ARN → name) used by findByARN.
func environmentARNIndexKeyFn(v *Environment) string { return regionKey(v.region, v.ARN) }

// registerAllTables registers the backend's resource table exactly once. It
// must be called during construction only (immediately after b.registry is
// created), never on every Reset() -- store.Register panics on a duplicate
// name, so runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.environments = store.Register(b.registry, "environments", store.New(environmentKeyFn))
	b.environmentsByRegion = b.environments.AddIndex("byRegion", environmentRegionIndexKeyFn)
	b.environmentsByARN = b.environments.AddIndex("byARN", environmentARNIndexKeyFn)
}
