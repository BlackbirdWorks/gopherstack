package stepfunctions

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (12e611a4) / sqs (0f09d77c) conversions this follows.
//
// Three secondary store.Index instances group child resources by their
// (immutable) parent state machine or execution ARN, replacing the
// smVersions / smExecutions / execMapRuns hand-maintained []string index
// maps:
//   - executionsByStateMachine (on executions, keyed by StateMachineArn)
//   - versionsByStateMachine (on versions, keyed by StateMachineArn)
//   - mapRunsByExecution (on mapRuns, keyed by ExecutionArn)
//
// Execution.Status is deliberately NOT indexed (see smExecsByStatus in
// backend.go, which remains a hand-maintained map). Status is mutated in
// place by StopExecution/RedriveExecution/runParsedExecution without a
// matching Table.Put, so an Index keyed by Status would go stale the moment
// it changed -- see store.Index.AddIndex's doc comment on mutable index
// keys, and services/route53/backend.go's ListTrafficPolicyInstancesByPolicy
// for the same reasoning applied elsewhere in this codebase.
//
// StateMachineAlias has no StateMachineArn field of its own (only the
// composite StateMachineAliasArn), so it cannot be indexed by a pure
// function of its own fields either; smAliases remains a hand-maintained
// []string index map for the same reason EC2 leaves keys that are not a pure
// function of the value's own fields as plain maps (see
// services/ec2/store_setup.go's comment above registerAllTables).
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func stateMachinesKeyFn(v *StateMachine) string               { return v.StateMachineArn }
func executionsKeyFn(v *Execution) string                     { return v.ExecutionArn }
func activitiesKeyFn(v *Activity) string                      { return v.ActivityArn }
func stateMachineVersionsKeyFn(v *StateMachineVersion) string { return v.StateMachineVersionArn }
func stateMachineAliasesKeyFn(v *StateMachineAlias) string    { return v.StateMachineAliasArn }
func mapRunsKeyFn(v *MapRun) string                           { return v.MapRunArn }

func executionsByStateMachineKeyFn(v *Execution) string         { return v.StateMachineArn }
func versionsByStateMachineKeyFn(v *StateMachineVersion) string { return v.StateMachineArn }
func mapRunsByExecutionKeyFn(v *MapRun) string                  { return v.ExecutionArn }

// registerAllTables registers every converted resource map on b.registry
// exactly once, plus the three secondary indexes documented above. It must
// be called during construction only (immediately after b.registry is
// created), never on every Reset() -- store.Register panics on a duplicate
// name, so runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.resetLocked in backend.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// (plus any secondary store.AddIndex) call to the concrete field and value
// type. A closure list keeps registerAllTables small and uniform regardless
// of how many resource tables the backend grows to, matching
// services/ec2/store_setup.go's tableRegistrations.
//
//nolint:gochecknoglobals // registration table, analogous to ec2/store_setup.go's tableRegistrations
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.stateMachines = store.Register(b.registry, "stateMachines", store.New(stateMachinesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.executions = store.Register(b.registry, "executions", store.New(executionsKeyFn))
		b.executionsByStateMachine = b.executions.AddIndex("byStateMachine", executionsByStateMachineKeyFn)
	},
	func(b *InMemoryBackend) {
		b.activities = store.Register(b.registry, "activities", store.New(activitiesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.versions = store.Register(b.registry, "versions", store.New(stateMachineVersionsKeyFn))
		b.versionsByStateMachine = b.versions.AddIndex("byStateMachine", versionsByStateMachineKeyFn)
	},
	func(b *InMemoryBackend) {
		b.aliases = store.Register(b.registry, "aliases", store.New(stateMachineAliasesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.mapRuns = store.Register(b.registry, "mapRuns", store.New(mapRunsKeyFn))
		b.mapRunsByExecution = b.mapRuns.AddIndex("byExecution", mapRunsByExecutionKeyFn)
	},
}
