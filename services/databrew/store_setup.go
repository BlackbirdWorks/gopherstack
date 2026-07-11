package databrew

// Code in this file supports Phase 3.3 of the datalayer refactor: resource
// maps with a pure per-value identity are backed by *store.Table[T] instead
// of a hand-rolled map[string]*T. See pkgs/store's package doc and the
// services/sesv2, services/dax, services/waf (clean direct-registration)
// conversions this follows.
//
// # Why this looks like services/pipes/services/eventbridge, not services/sesv2
//
// sesv2/dax/waf each model ONE region per InMemoryBackend instance, so a flat
// map[string]*store.Table[T] statically registered once at construction time
// is enough. databrew's InMemoryBackend, like pipes' and eventbridge's, holds
// every region's state in one instance (region -> resource), so each
// convertible resource keeps its existing outer region-keyed map, whose
// values become a *store.Table[T] created lazily on first access -- exactly
// mirroring the lazy map creation the hand-rolled *Store helpers did before
// conversion. The first time a table is created it is ALSO registered on
// b.registry under a name that encodes its region (e.g. "datasets/us-east-1")
// so that persistence.go can still drive Snapshot/Restore through one
// *store.Registry regardless of how many regions exist -- see
// preRegisterSnapshotTables in persistence.go.
//
// # What did NOT convert
//
// jobRuns (map[string]map[string][]*JobRun, region -> job name -> run
// history) stays a plain map. Each job's run history is an ORDER-SENSITIVE
// slice: StartJobRun appends in chronological order and ListJobRuns reverses
// it to return newest-first, and store.Index groups are NOT insertion-order
// stable (removal is swap-with-last) -- there is also no *T identity to key a
// store.Table by (a []*JobRun is a slice, not a resource with its own key).
// jobRuns is persisted directly as a raw field on backendSnapshot instead
// (see persistence.go).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func datasetKeyFn(v *Dataset) string   { return v.Name }
func recipeKeyFn(v *Recipe) string     { return v.Name }
func projectKeyFn(v *Project) string   { return v.Name }
func jobKeyFn(v *Job) string           { return v.Name }
func rulesetKeyFn(v *Ruleset) string   { return v.Name }
func scheduleKeyFn(v *Schedule) string { return v.Name }

// snapshotKeys returns t's values' keys in the same deterministic,
// key-sorted order as [store.Table.Snapshot] -- the store.Table analogue of
// the sortedKeys(map[string]V) helper the hand-rolled maps used, reused by
// every List* backend method to build the key list paginateKeys operates on.
func snapshotKeys[V any](t *store.Table[V], keyFn func(*V) string) []string {
	items := t.Snapshot()

	keys := make([]string, len(items))
	for i, v := range items {
		keys[i] = keyFn(v)
	}

	return keys
}

// datasetsTable returns the *store.Table[Dataset] for region, lazily creating
// (and registering on b.registry) the first time that region is touched.
func (b *InMemoryBackend) datasetsTable(region string) *store.Table[Dataset] {
	t, ok := b.datasets[region]
	if !ok {
		t = store.Register(b.registry, "datasets/"+region, store.New(datasetKeyFn))
		b.datasets[region] = t
	}

	return t
}

// recipesTable returns the *store.Table[Recipe] for region, lazily creating
// (and registering on b.registry) the first time that region is touched.
func (b *InMemoryBackend) recipesTable(region string) *store.Table[Recipe] {
	t, ok := b.recipes[region]
	if !ok {
		t = store.Register(b.registry, "recipes/"+region, store.New(recipeKeyFn))
		b.recipes[region] = t
	}

	return t
}

// projectsTable returns the *store.Table[Project] for region, lazily
// creating (and registering on b.registry) the first time that region is
// touched.
func (b *InMemoryBackend) projectsTable(region string) *store.Table[Project] {
	t, ok := b.projects[region]
	if !ok {
		t = store.Register(b.registry, "projects/"+region, store.New(projectKeyFn))
		b.projects[region] = t
	}

	return t
}

// jobsTable returns the *store.Table[Job] for region, lazily creating (and
// registering on b.registry) the first time that region is touched.
func (b *InMemoryBackend) jobsTable(region string) *store.Table[Job] {
	t, ok := b.jobs[region]
	if !ok {
		t = store.Register(b.registry, "jobs/"+region, store.New(jobKeyFn))
		b.jobs[region] = t
	}

	return t
}

// rulesetsTable returns the *store.Table[Ruleset] for region, lazily
// creating (and registering on b.registry) the first time that region is
// touched.
func (b *InMemoryBackend) rulesetsTable(region string) *store.Table[Ruleset] {
	t, ok := b.rulesets[region]
	if !ok {
		t = store.Register(b.registry, "rulesets/"+region, store.New(rulesetKeyFn))
		b.rulesets[region] = t
	}

	return t
}

// schedulesTable returns the *store.Table[Schedule] for region, lazily
// creating (and registering on b.registry) the first time that region is
// touched.
func (b *InMemoryBackend) schedulesTable(region string) *store.Table[Schedule] {
	t, ok := b.schedules[region]
	if !ok {
		t = store.Register(b.registry, "schedules/"+region, store.New(scheduleKeyFn))
		b.schedules[region] = t
	}

	return t
}
