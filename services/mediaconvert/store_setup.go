package mediaconvert

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// backend-level map[string]*T resource field on InMemoryBackend is replaced
// with a *store.Table[T], following the pattern established by services/ses
// (commit history referenced in services/ses/store_setup.go) for value types
// whose identity can't round-trip a direct JSON marshal, and
// services/medialive/services/sesv2 for "clean" direct registration. See
// pkgs/store's package doc for the underlying primitive.
//
// Tables split into three groups:
//
//   - "Clean" tables (queues, jobTemplates, jobs, presets) key off a field
//     the value type already carries as a real (non-json:"-") field --
//     Queue.Name, JobTemplate.Name, Job.ID, Preset.Name -- so all four are
//     registered on b.registry here, and persistence.go drives them through
//     b.registry.SnapshotAll() / RestoreAll() directly.
//   - "Dirty" tables (queueCounters, tokenIndex) key off a field with no
//     natural home on the value type: queueJobCounter and tokenEntry both
//     predate this refactor with every field unexported (so neither ever
//     round-tripped through JSON -- see their doc comments in backend.go).
//     Each gained an unexported identity field (queueArn / token) purely so
//     store.Table's keyFn can derive a key from the value. They are NOT
//     registered on b.registry: persistence.go instead builds a throwaway DTO
//     store.Registry (mirroring services/ses) whose DTO types carry the
//     identity as a real JSON field, so Snapshot/Restore never depends on an
//     unexported field to round-trip.
//   - queries is a *store.Table[jobsQuery] for keyed Get/Put/Delete access,
//     but unlike the tables above it is never persisted: StartJobsQuery
//     results were never part of backendSnapshot pre-Phase-3.3 either (see
//     the old Restore, which always reset b.queries to empty). It is
//     constructed via store.New only -- not registered on b.registry and not
//     round-tripped through the dirty DTO registry -- so it is reset
//     alongside the others (resetTablesLocked, backend.go) but never
//     serialized.
//
// queuesByArn is a secondary store.Index over queues grouping by Arn,
// replacing the reverse map[string]*Queue lookup the previous
// implementation maintained by hand on every Put/Delete.
//
// Left as plain maps (not store.Table-backed), each audited against the
// pre-refactor persistence.go / backend.go for its persisted status:
//   - tags (map[string]map[string]string): values are plain strings, not
//     *T, so they do not fit store.Table's keyed-by-identity-value shape.
//     Was persisted before -- still persisted, unchanged.
//   - certificates (map[string]struct{}): values are the empty struct, not
//     *T. Was persisted before -- still persisted, unchanged.
//
// policy (*Policy) is a single scalar field, not a map, and is untouched by
// this refactor.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func queueKeyFn(v *Queue) string { return v.Name }

func queueArnKeyFn(v *Queue) string { return v.Arn }

func jobTemplateKeyFn(v *JobTemplate) string { return v.Name }

func jobKeyFn(v *Job) string { return v.ID }

func presetKeyFn(v *Preset) string { return v.Name }

func queueCounterKeyFn(v *queueJobCounter) string { return v.queueArn }

func tokenEntryKeyFn(v *tokenEntry) string { return v.token }

func jobsQueryKeyFn(v *jobsQuery) string { return v.queryID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. "Clean" tables are
// registered on b.registry; the "dirty" tables and the ephemeral queries
// table are constructed alongside them but deliberately NOT registered --
// see the file doc comment above. It must be called during construction
// only, never on every Reset(): store.Register panics on a duplicate name,
// so runtime resets go through resetTablesLocked (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.queues = store.Register(b.registry, "queues", store.New(queueKeyFn))
	b.queuesByArn = b.queues.AddIndex("byARN", queueArnKeyFn)
	b.jobTemplates = store.Register(b.registry, "jobTemplates", store.New(jobTemplateKeyFn))
	b.jobs = store.Register(b.registry, "jobs", store.New(jobKeyFn))
	b.presets = store.Register(b.registry, "presets", store.New(presetKeyFn))

	b.queueCounters = store.New(queueCounterKeyFn)
	b.tokenIndex = store.New(tokenEntryKeyFn)

	b.queries = store.New(jobsQueryKeyFn)
}
