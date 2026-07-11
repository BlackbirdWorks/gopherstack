package comprehend

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T]. See pkgs/store's package doc for the underlying
// primitive, and services/sesv2/store_setup.go (commit history for Phase
// 3.3) for the "clean, direct" pattern this file follows.
//
// jobs, resources, and iterations each already carry their own identity as a
// real (non-json:"-") field -- Job.JobID, Resource.Arn,
// FlywheelIteration.FlywheelIterationID -- so all three are "clean" tables
// registered directly on b.registry; no ephemeral DTO-registry indirection
// (the services/ses/services/codecommit "dirty" pattern) is needed. None of
// the three need a secondary store.Index either: every filtered listing
// (ListJobs by JobType, ListResources by Type, ListFlywheelIterations by
// FlywheelArn) scans the table's full contents exactly as the original
// map-range loop did, and the fields advanced in place by
// advanceJob/advanceTrainingResource/GetFlywheelIteration (JobStatus,
// Resource.Status, FlywheelIterationStatus, etc.) are never used as an index
// key, so mutating them through a pointer obtained from Table.Get needs no
// Delete+Put dance.
//
// Left as plain maps (not store.Table-backed):
//   - tags (map[string]map[string]string): values are plain string maps, not
//     *T, so they do not fit store.Table's keyed-by-identity-value shape.
//   - policies, policyRevisions (map[string]string): scalar values, same
//     reason.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func jobKeyFn(v *Job) string { return v.JobID }

func resourceKeyFn(v *Resource) string { return v.Arn }

func flywheelIterationKeyFn(v *FlywheelIteration) string { return v.FlywheelIterationID }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.jobs = store.Register(b.registry, "jobs", store.New(jobKeyFn))
	b.resources = store.Register(b.registry, "resources", store.New(resourceKeyFn))
	b.iterations = store.Register(b.registry, "iterations", store.New(flywheelIterationKeyFn))
}
