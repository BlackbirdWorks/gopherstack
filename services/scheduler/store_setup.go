package scheduler

// Code in this file supports Phase 3.3 of the datalayer refactor: the two
// region-nested resource collections (previously map[region]map[key]*T, with a
// companion map[region]map[ARN]key reverse index each) are replaced by a flat
// *store.Table[T] keyed by the composite "region|id" string (see regionKey in
// backend.go), with companion *store.Index values: one grouping entries by
// region (for per-region list scans, replacing the old outer map nesting) and
// one grouping entries by "region|ARN" (replacing the old per-region ARN
// reverse map used by TagResource/UntagResource/ListTagsForResource). It
// otherwise follows pkgs/store's package doc and the services/mwaa /
// services/codepipeline (region-qualified-table) conversions.
//
// Both tables are "dirty" in the persistence sense -- each value carries a live
// *tags.Tags field (a Prometheus-instrumented map) that plain json.Marshal
// cannot round-trip while preserving the per-resource metric name -- so neither
// is registered on a shared store.Registry; each is built with store.New only,
// and persistence.go handles Snapshot/Restore via a plain-map DTO (see that
// file). Reset (backend.go) resets each table explicitly.
//
// Schedule carries its own exported Region field (part of the AWS wire shape),
// so the schedules table keys directly off it. ScheduleGroup has no region
// field; its region is always embedded in its ARN (see CreateScheduleGroup /
// seedDefaultGroup), so the scheduleGroups key functions derive the region from
// the ARN via the backend's scheduleGroupRegionOf helper (bound as closures
// below because they need the backend's default-region fallback).

import "github.com/blackbirdworks/gopherstack/pkgs/store"

// schedulesKeyFn derives the composite primary key for the flat schedules table
// from the schedule's own Region field and its group/name composite.
func schedulesKeyFn(v *Schedule) string {
	return regionKey(v.Region, scheduleKey(v.GroupName, v.Name))
}

// schedulesRegionIndexKeyFn groups schedules by region alone, replacing the old
// outer map[region] nesting for per-region scans (ListSchedules, cascade
// delete, runner enumeration).
func schedulesRegionIndexKeyFn(v *Schedule) string { return v.Region }

// schedulesARNIndexKeyFn groups schedules by "region|ARN", replacing the old
// per-region ARN reverse map used by the tag operations.
func schedulesARNIndexKeyFn(v *Schedule) string { return regionKey(v.Region, v.ARN) }

// registerAllTables builds the backend's two resource tables and their indexes
// exactly once, during construction. Unlike the clean-register conversions
// these tables are NOT placed on a store.Registry (they are persistence-dirty;
// see the file doc), so this only wires the fields -- runtime resets go through
// each table's own Reset() (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.schedules = store.New(schedulesKeyFn)
	b.schedulesByRegion = b.schedules.AddIndex("byRegion", schedulesRegionIndexKeyFn)
	b.schedulesByARN = b.schedules.AddIndex("byARN", schedulesARNIndexKeyFn)

	b.scheduleGroups = store.New(b.scheduleGroupTableKeyFn)
	b.scheduleGroupsByRegion = b.scheduleGroups.AddIndex(
		"byRegion",
		func(g *ScheduleGroup) string { return b.scheduleGroupRegionOf(g) },
	)
	b.scheduleGroupsByARN = b.scheduleGroups.AddIndex("byARN", b.scheduleGroupARNIndexKeyFn)
}
