package polly

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 and
// services/dax. See pkgs/store's package doc for the underlying primitive.
//
// Both convertible fields here already carry their own identity as a real
// (non-json:"-") field -- Lexicon.Name and SpeechSynthesisTask.TaskID were
// already set consistently at every write site before this refactor. So both
// are "clean" tables, registered directly on b.registry, with persistence.go
// driving them through b.registry.SnapshotAll() / RestoreAll().
//
// Left as a plain map (not store.Table-backed): tags (map[string]map[string]string)
// -- its values are plain map[string]string, not *T, so there is nothing for
// store.Table to key on. See persistence.go for how it is persisted directly.
//
// voices ([]Voice) is the static built-in voice catalogue, not a resource
// map, and is left untouched by this refactor.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func lexiconKeyFn(v *Lexicon) string { return v.Name }

func speechSynthesisTaskKeyFn(v *SpeechSynthesisTask) string { return v.TaskID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.lexicons = store.Register(b.registry, "lexicons", store.New(lexiconKeyFn))
	b.tasks = store.Register(b.registry, "tasks", store.New(speechSynthesisTaskKeyFn))
}
