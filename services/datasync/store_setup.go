package datasync

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 /
// services/opsworks (data-driven, direct registration -- see pkgs/store's
// package doc for the underlying primitive).
//
// agents, locations, and tasks are already flat ARN-keyed resources with
// globally unique identity (storedAgent.AgentArn, storedLocation.LocationArn,
// storedTask.TaskArn), so all three are "clean" tables registered directly on
// b.registry -- no synthetic composite key is introduced.
//
// executions was formerly a nested map[string]map[string]*storedTaskExecution
// (taskArn -> executionArn -> execution). storedTaskExecution.TaskExecutionArn
// is itself globally unique -- it embeds the owning TaskArn
// (".../task/<id>/execution/<id>"), so it fits the same "flat resource with
// globally-unique ID" shape as agents/locations/tasks: register it directly,
// keyed by TaskExecutionArn. The former taskArn-scoped nesting becomes the
// executionsByTask secondary *store.Index, whose key function
// (extractTaskArnFromExecution) derives the parent TaskArn purely from the
// execution's own ARN -- a pure function of the value's identity, matching
// the opsworks byStack precedent.
//
// Left as a plain map (not store.Table-backed): tags (map[string]map[string]string)
// -- its values are plain string maps, not *T, so it does not fit
// store.Table's keyed-by-identity-value shape. See persistence.go.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func agentKeyFn(v *storedAgent) string { return v.AgentArn }

func locationKeyFn(v *storedLocation) string { return v.LocationArn }

func taskKeyFn(v *storedTask) string { return v.TaskArn }

func executionKeyFn(v *storedTaskExecution) string { return v.TaskExecutionArn }

func executionTaskKeyFn(v *storedTaskExecution) string {
	return extractTaskArnFromExecution(v.TaskExecutionArn)
}

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.agents = store.Register(b.registry, "agents", store.New(agentKeyFn))

	b.locations = store.Register(b.registry, "locations", store.New(locationKeyFn))

	b.tasks = store.Register(b.registry, "tasks", store.New(taskKeyFn))

	b.executions = store.Register(b.registry, "executions", store.New(executionKeyFn))
	b.executionsByTask = b.executions.AddIndex("byTask", executionTaskKeyFn)
}
