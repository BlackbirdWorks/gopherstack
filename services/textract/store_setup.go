package textract

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]map[string]*T resource field on InMemoryBackend (jobs,
// expenseJobs, lendingJobs, adapters, adapterVersions) is replaced with a
// *store.Table[T], following the pattern established by services/wafv2
// (region-nested composite keys) and services/ses/services/sqs (DTO-registry
// for identity fields that don't round-trip through a direct JSON marshal).
// See pkgs/store's package doc for the underlying primitive.
//
// Every one of these five maps used to live in a map[region]map[key]*T:
// region was the outer partition providing full isolation between
// same-named/same-ID resources in different regions (see isolation_test.go).
// store.Table only supports a single string key, so the two-level nesting is
// flattened into one composite key via regionKey(region, innerKey) -- a
// mechanical shape change only; every lookup reconstructs exactly the same
// address the old nested map would have.
//
// None of DocumentJob/ExpenseJob/LendingJob/Adapter/AdapterVersion carried a
// field recording which region bucket they lived in (that information was
// implicit in the outer map key), so each gained a Region field tagged
// `json:"-"` purely so store.Table's keyFn can derive a region-qualified key
// from the value -- see the field doc comments in backend.go. None of the
// five tables is registered on a persistent b.registry: persistence.go
// instead builds a throwaway DTO store.Registry per Snapshot/Restore call
// whose DTO types carry Region as a real JSON field, so Snapshot/Restore
// never depends on a json:"-" field to round-trip correctly (mirrors
// services/wafv2's "dirty" tables).
//
// clientTokenToJobID/adapterClientTokenToID stay plain region-nested maps:
// their values are strings, not *T, so they do not fit store.Table's
// keyed-by-identity-value shape and are unaffected by this conversion.
//
// adapterVersions additionally carries a secondary store.Index
// (adapterVersionsByAdapter) grouping by (region, adapterID), replacing the
// old "scan the region-scoped adapterVersions map for AdapterID==X" pattern
// DeleteAdapter/ListAdapterVersions used.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

// regionKey builds the composite store.Table/Index key used throughout this
// backend to flatten what used to be a map[region]map[key]*T nested map into
// a single map[key]*T (see pkgs/store's package doc). It is the direct
// mechanical replacement for indexing into the region bucket first: any call
// site that used to write `someMap[region][key]` now uses
// `someTable.Get(regionKey(region, key))`.
func regionKey(region, key string) string {
	return region + "|" + key
}

func jobKey(j *DocumentJob) string       { return regionKey(j.Region, j.JobID) }
func jobRegionKey(j *DocumentJob) string { return j.Region }

func expenseJobKey(j *ExpenseJob) string       { return regionKey(j.Region, j.JobID) }
func expenseJobRegionKey(j *ExpenseJob) string { return j.Region }

func lendingJobKey(j *LendingJob) string       { return regionKey(j.Region, j.JobID) }
func lendingJobRegionKey(j *LendingJob) string { return j.Region }

func adapterKey(a *Adapter) string       { return regionKey(a.Region, a.AdapterID) }
func adapterRegionKey(a *Adapter) string { return a.Region }

// adapterVersionTableKey is the store.Table[AdapterVersion] primary key:
// region plus the existing adapterID#version composite (adapterVersionKey,
// defined in backend.go and also used to build AdapterVersion ARNs).
func adapterVersionTableKey(av *AdapterVersion) string {
	return regionKey(av.Region, adapterVersionKey(av.AdapterID, av.AdapterVersion))
}

// adapterVersionAdapterKey groups versions by (region, adapterID).
func adapterVersionAdapterKey(av *AdapterVersion) string {
	return regionKey(av.Region, av.AdapterID)
}

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. None of these five tables are
// registered on a persistent b.registry (see file doc comment above);
// persistence.go builds an ephemeral DTO registry instead. It must be called
// during construction only, never on every Reset(): runtime resets go
// through resetTablesLocked instead.
func registerAllTables(b *InMemoryBackend) {
	b.jobs = store.New(jobKey)
	b.jobsByRegion = b.jobs.AddIndex("region", jobRegionKey)

	b.expenseJobs = store.New(expenseJobKey)
	b.expenseJobsByRegion = b.expenseJobs.AddIndex("region", expenseJobRegionKey)

	b.lendingJobs = store.New(lendingJobKey)
	b.lendingJobsByRegion = b.lendingJobs.AddIndex("region", lendingJobRegionKey)

	b.adapters = store.New(adapterKey)
	b.adaptersByRegion = b.adapters.AddIndex("region", adapterRegionKey)

	b.adapterVersions = store.New(adapterVersionTableKey)
	b.adapterVersionsByAdapter = b.adapterVersions.AddIndex("adapter", adapterVersionAdapterKey)
}

// resetTablesLocked resets every store.Table this backend owns. Must be
// called with b.mu held for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.jobs.Reset()
	b.expenseJobs.Reset()
	b.lendingJobs.Reset()
	b.adapters.Reset()
	b.adapterVersions.Reset()
}
