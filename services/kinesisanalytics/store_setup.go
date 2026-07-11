package kinesisanalytics

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// map[string]map[string]*Application fields (apps, appsByARN) on
// InMemoryBackend -- nested by region, then by application name / ARN -- are
// replaced with a single *store.Table[Application] plus two secondary
// [store.Index]es, following the pattern established by services/kinesis
// (composite "region#name" primary key, byRegion index replacing the old
// per-region map iteration) and services/kinesisanalyticsv2 (the v2 sibling
// converted in the same rollout, which uses the identical
// applicationKey/byRegion/byARN shape).
//
// applications is keyed by the composite "region#applicationName": a bare
// ApplicationName is only unique within a region (the pre-Phase-3.3 layout
// nested a map[name]*Application per region), and every access site already
// resolves the region from context before doing a name lookup, so the
// composite-key rule applies. Application gained an additive Region field
// purely so the table's keyFn (and the byRegion index) can derive both
// halves of the key from the value itself, mirroring how ApplicationName was
// already a real field. Region and ApplicationARN are set once at
// CreateApplication time and never mutated afterward, so neither index ever
// goes stale from an in-place field mutation (see pkgs/store's Index docs on
// that hazard) -- only Table.Delete (on final DELETING cleanup) touches the
// indexes after insertion.
//
// byRegion replaces the old map[region]map[name]*Application region-scoped
// iteration ListApplications/CreateApplication's per-region count performed.
// byARN replaces the old map[region]map[arn]*Application reverse index used
// by ListTagsForResource/TagResource/UntagResource: because an
// ApplicationARN already embeds its region (see applicationARN in
// backend.go), a byARN lookup is unambiguous without any region-scoping of
// its own.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// applicationKey builds the composite "<region>#<applicationName>" key
// shared by every application. Access sites use this directly so the exact
// same key is used for Put/Get/Has/Delete.
func applicationKey(region, name string) string {
	return region + "#" + name
}

// applicationKeyFn is the [store.Table] primary-key function for b.apps.
func applicationKeyFn(v *Application) string { return applicationKey(v.Region, v.ApplicationName) }

// applicationRegionKeyFn is the [store.Index] key function for the byRegion
// index.
func applicationRegionKeyFn(v *Application) string { return v.Region }

// applicationARNKeyFn is the [store.Index] key function for the byARN index.
func applicationARNKeyFn(v *Application) string { return v.ApplicationARN }

// registerAllTables registers the backend's Application table and its
// secondary indexes exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.apps = store.Register(b.registry, "apps", store.New(applicationKeyFn))
	b.appsByRegion = b.apps.AddIndex("byRegion", applicationRegionKeyFn)
	b.appsByARN = b.apps.AddIndex("byARN", applicationARNKeyFn)
}
