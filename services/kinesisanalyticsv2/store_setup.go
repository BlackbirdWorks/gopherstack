package kinesisanalyticsv2

// Code in this file supports Phase 3.3 of the datalayer refactor: the
// map[string]map[string]*T resource fields on InMemoryBackend (nested by
// region, then by resource name) are replaced with a *store.Table[T] each,
// following the pattern established by services/sesv2 (data-driven direct +
// composite registration) and services/sqs (register-on-b.registry for
// lifecycle even though the value type needs a separate DTO for
// Snapshot/Restore -- see pkgs/store's package doc for the underlying
// primitive).
//
// applications is keyed by the composite "region#applicationName": a bare
// ApplicationName is only unique within a region (the pre-Phase-3.3 layout
// nested a map[name]*Application per region), and every access site already
// resolves the region from context/ARN before doing a name lookup, so the
// composite-key rule applies. Application gained an additive Region
// json:"-" field purely so the table's keyFn (and the byRegion index) can
// derive both halves of the key from the value itself, mirroring how
// ApplicationName was already a real field. byRegion replaces the old
// map[region]map[name]*Application region-scoped iteration ListApplications
// performed; byARN replaces the old map[region]map[arn]name reverse index
// findByARN used for TagResource/UntagResource/ListTagsForResource.
//
// snapshots is likewise keyed by the composite "region#appName#snapshotName"
// (a SnapshotName is only unique within its owning application), with
// Snapshot gaining additive Region/AppName json:"-" fields for the same
// reason Application gained Region. byApp replaces the old
// map[region]map[appName][]*Snapshot grouping.
//
// Both tables carry several fields tagged json:"-" on the live struct
// (Application: CreatedAt, Region, Tags, CloudWatchLoggingOptionDescs,
// InputDescriptions, OutputDescriptions, ReferenceDataSourceDescriptions,
// VpcConfigurationDescriptions; Snapshot: SnapshotCreation, Region,
// AppName), so neither table's snapshotJSON can be used directly for
// persistence without silently dropping those fields -- see persistence.go
// for the ephemeral DTO registry (mirroring services/ses's "dirty" tables)
// that Snapshot/Restore uses instead. Both tables are still registered on
// b.registry here (matching services/sqs's Queue/moveTasks precedent) purely
// for lifecycle: Reset()/version-mismatch discard both go through
// b.registry.ResetAll() instead of a hand-written reset per table.
//
// operations and versions remain plain nested maps of slices -- see the
// InMemoryBackend doc comment in backend.go for why (order-sensitive, not a
// keyed-by-identity-value shape, and not persisted before or after this
// conversion).
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// applicationKey builds the composite "<region>#<applicationName>" key
// shared by every application. Access sites use this directly so the exact
// same key is used for Put/Get/Has/Delete.
func applicationKey(region, name string) string {
	return region + "#" + name
}

func applicationKeyFn(v *Application) string { return applicationKey(v.Region, v.ApplicationName) }

func applicationRegionKeyFn(v *Application) string { return v.Region }

func applicationARNKeyFn(v *Application) string { return v.ApplicationARN }

// appParentKey builds the composite "<region>#<appName>" key shared by every
// snapshot of a given application -- the same prefix [applicationKey] uses,
// reused here as the byApp index key (grouping, not primary-key lookup).
func appParentKey(region, appName string) string {
	return region + "#" + appName
}

// snapshotKey builds the composite "<region>#<appName>#<snapshotName>" key
// shared by every snapshot. Access sites use this directly so the exact same
// key is used for Put/Get/Has/Delete.
func snapshotKey(region, appName, snapshotName string) string {
	return appParentKey(region, appName) + "#" + snapshotName
}

func snapshotKeyFn(v *Snapshot) string { return snapshotKey(v.Region, v.AppName, v.SnapshotName) }

func snapshotAppKeyFn(v *Snapshot) string { return appParentKey(v.Region, v.AppName) }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.applications = store.Register(b.registry, "applications", store.New(applicationKeyFn))
	b.applicationsByRegion = b.applications.AddIndex("byRegion", applicationRegionKeyFn)
	b.applicationsByARN = b.applications.AddIndex("byARN", applicationARNKeyFn)

	b.snapshots = store.Register(b.registry, "snapshots", store.New(snapshotKeyFn))
	b.snapshotsByApp = b.snapshots.AddIndex("byApp", snapshotAppKeyFn)
}
