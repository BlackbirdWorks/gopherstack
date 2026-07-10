package translate

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 and
// services/dax. See pkgs/store's package doc for the underlying primitive.
//
// All three convertible fields here already carry their own identity as a
// real (non-json:"-") field -- Terminology.Name, ParallelData.Name, and
// TranslationJob.JobID were already set consistently at every write site
// before this refactor. So all three are "clean" tables, registered
// directly on b.registry, with persistence.go driving them through
// b.registry.SnapshotAll() / RestoreAll(). Names are globally unique (not
// scoped to any parent), so none need a composite key.
//
// Left as a plain map (not store.Table-backed): tags
// (map[string]map[string]string) -- its values are plain map[string]string,
// not *T, so there is nothing for store.Table to key on. See persistence.go
// for how it is persisted directly.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func terminologyKeyFn(v *Terminology) string { return v.Name }

func parallelDataKeyFn(v *ParallelData) string { return v.Name }

func translationJobKeyFn(v *TranslationJob) string { return v.JobID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.terminologies = store.Register(b.registry, "terminologies", store.New(terminologyKeyFn))
	b.parallelData = store.Register(b.registry, "parallelData", store.New(parallelDataKeyFn))
	b.jobs = store.Register(b.registry, "jobs", store.New(translationJobKeyFn))
}
