package swf

// Code in this file supports Phase 3.3 of the datalayer refactor: the four
// domain-nested resource collections (domains, workflows, activities,
// executions -- previously flat maps keyed by a hand-built composite string)
// are each replaced by a *store.Table[T]. domains needs no composite key (a
// domain name is already globally unique); workflows, activities, and
// executions keep the exact same composite-key shape their old map keys used
// (domain+":"+name+":"+version, domain+":"+workflowID), since Domain,
// Name/WorkflowID, and Version are already real, wire-visible JSON fields on
// each value type -- no hidden field is needed, so all four are "clean"
// tables registered directly on b.registry.
//
// workflows, activities, and executions additionally gain a companion
// byDomain *store.Index grouping entries by domain, replacing the linear
// full-table scan+filter every domain-scoped List/Count operation used
// against the old flat map.
//
// activeActivityTasks and activeDecisionTasks are "dirty" tables: their key
// (taskToken) has no home on the record type's wire shape, so each record
// gained a TaskToken field tagged json:"-" purely for store.Table's keyFn
// (see the type docs in models.go). Both are deliberately NOT
// store.Register-ed onto b.registry (so b.registry.SnapshotAll never tries to
// marshal them directly, which would silently drop the hidden field);
// persistence.go instead builds an ephemeral DTO registry for them, mirroring
// services/ses's "dirty" identities/configSets/... tables.
//
// history, activityQueues, decisionQueues, and tags are deliberately left as
// plain maps -- see the InMemoryBackend doc comment in store.go.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func domainKeyFn(v *Domain) string { return v.Name }

func workflowTypeKeyFn(v *WorkflowType) string {
	return v.Domain + ":" + v.Name + ":" + v.Version
}
func workflowTypeDomainIndexKeyFn(v *WorkflowType) string { return v.Domain }

func activityTypeKeyFn(v *ActivityType) string {
	return v.Domain + ":" + v.Name + ":" + v.Version
}
func activityTypeDomainIndexKeyFn(v *ActivityType) string { return v.Domain }

func workflowExecutionKeyFn(v *WorkflowExecution) string {
	return v.Domain + ":" + v.WorkflowID
}
func workflowExecutionDomainIndexKeyFn(v *WorkflowExecution) string { return v.Domain }

func activeActivityTaskKeyFn(v *activeActivityTaskRecord) string { return v.TaskToken }

func activeDecisionTaskKeyFn(v *activeDecisionTaskRecord) string { return v.TaskToken }

// registerAllTables registers every converted resource collection on
// b.registry (the "clean" tables) or builds it standalone (the "dirty"
// activeActivityTasks/activeDecisionTasks tables -- see the file doc comment
// above) exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend), never
// on every Reset() -- store.Register panics on a duplicate name, so runtime
// resets go through b.registry.ResetAll() plus the dirty tables' own Reset()
// calls instead (see InMemoryBackend.Reset in store.go).
func registerAllTables(b *InMemoryBackend) {
	b.domains = store.Register(b.registry, "domains", store.New(domainKeyFn))

	b.workflows = store.Register(b.registry, "workflows", store.New(workflowTypeKeyFn))
	b.workflowsByDomain = b.workflows.AddIndex("byDomain", workflowTypeDomainIndexKeyFn)

	b.activities = store.Register(b.registry, "activities", store.New(activityTypeKeyFn))
	b.activitiesByDomain = b.activities.AddIndex("byDomain", activityTypeDomainIndexKeyFn)

	b.executions = store.Register(b.registry, "executions", store.New(workflowExecutionKeyFn))
	b.executionsByDomain = b.executions.AddIndex("byDomain", workflowExecutionDomainIndexKeyFn)

	b.activeActivityTasks = store.New(activeActivityTaskKeyFn)
	b.activeDecisionTasks = store.New(activeDecisionTaskKeyFn)
}
