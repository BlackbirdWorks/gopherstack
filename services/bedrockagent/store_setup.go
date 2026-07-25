package bedrockagent

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T (and map[string]map[string]*T) resource field on
// InMemoryBackend is replaced with a *store.Table[T], following the pattern
// established by services/sesv2 / services/mediatailor / services/codecommit
// (data-driven, direct/composite registration -- see pkgs/store's package
// doc for the underlying primitive).
//
// agents, knowledgeBases, flows, and prompts are each keyed by their own
// real *ID field -- flat resources with IDs that are unique service-wide --
// so all four are "clean" direct registrations.
//
// agentVersions, actionGroups, agentAliases, agentCollaborators,
// agentKBAssocs, dataSources, ingestionJobs, flowVersions, flowAliases,
// promptVersions, and kbDocuments are all parent-scoped: e.g. an agent
// version number is only unique within its owning agent, an action group ID
// only within its owning (agent, agentVersion) pair, a data source only
// within its owning knowledge base -- every Get/Update/Delete call takes the
// full (parent[, grandparent], child) key together, never the child ID
// alone. That matches the composite-key rule (id unique only within parent,
// access always parent-scoped), so each keeps the same "parent/child[/
// grandchild]" composite key the original nested/prefix-scanned map already
// used, with an additive secondary Index replacing the O(n) linear
// prefix-scan or nested-map lookup the original Delete*/List* methods
// performed.
//
// Left as plain maps (not store.Table-backed): agentsByName, kbsByName,
// flowsByName, promptsByName (map[string]string reverse-lookup caches --
// their values are bare strings, not *T); agentVersionCtrs, flowVersionCtrs,
// promptVersionCtrs (map[string]int counters); and tags
// (map[string]map[string]string). See persistence.go for how each is
// carried through Snapshot/Restore.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

func agentKeyFn(v *Agent) string { return v.AgentID }

func agentVersionKeyFn(v *AgentVersion) string        { return agentVersionKey(v.AgentID, v.AgentVersion) }
func agentVersionByAgentKeyFn(v *AgentVersion) string { return v.AgentID }

func actionGroupKeyFn(v *AgentActionGroup) string {
	return agActionGroupKey(v.AgentID, v.AgentVersion, v.ActionGroupID)
}
func actionGroupByAgentVersionKeyFn(v *AgentActionGroup) string {
	return agentVersionScope(v.AgentID, v.AgentVersion)
}

func agentAliasKeyFn(v *AgentAlias) string        { return aliasKey(v.AgentID, v.AgentAliasID) }
func agentAliasByAgentKeyFn(v *AgentAlias) string { return v.AgentID }

func agentCollaboratorKeyFn(v *AgentCollaborator) string {
	return agentCollabKey(v.AgentID, v.AgentVersion, v.CollaboratorID)
}
func agentCollaboratorByAgentVersionKeyFn(v *AgentCollaborator) string {
	return agentVersionScope(v.AgentID, v.AgentVersion)
}

func knowledgeBaseKeyFn(v *KnowledgeBase) string { return v.KnowledgeBaseID }

func agentKBKeyFn(v *AgentKnowledgeBase) string {
	return agKBKey(v.AgentID, v.AgentVersion, v.KnowledgeBaseID)
}
func agentKBByAgentVersionKeyFn(v *AgentKnowledgeBase) string {
	return agentVersionScope(v.AgentID, v.AgentVersion)
}

func dataSourceKeyFn(v *DataSource) string     { return dsKey(v.KnowledgeBaseID, v.DataSourceID) }
func dataSourceByKBKeyFn(v *DataSource) string { return v.KnowledgeBaseID }

func ingestionJobKeyFn(v *IngestionJob) string {
	return jobKey(v.KnowledgeBaseID, v.DataSourceID, v.IngestionJobID)
}
func ingestionJobByDataSourceKeyFn(v *IngestionJob) string {
	return dsKey(v.KnowledgeBaseID, v.DataSourceID)
}

func flowKeyFn(v *Flow) string { return v.FlowID }

func flowVersionKeyFn(v *FlowVersion) string       { return flowVersionKey(v.FlowID, v.Version) }
func flowVersionByFlowKeyFn(v *FlowVersion) string { return v.FlowID }

func flowAliasKeyFn(v *FlowAlias) string       { return flowAliasKey(v.FlowID, v.AliasID) }
func flowAliasByFlowKeyFn(v *FlowAlias) string { return v.FlowID }

func promptKeyFn(v *Prompt) string { return v.PromptID }

func promptVersionKeyFn(v *PromptVersion) string         { return promptVersionKey(v.PromptID, v.Version) }
func promptVersionByPromptKeyFn(v *PromptVersion) string { return v.PromptID }

func kbDocumentKeyFn(v *KBDocumentDetail) string {
	return kbDocKey(v.KnowledgeBaseID, v.DataSourceID, v.DocumentID)
}
func kbDocumentByDataSourceKeyFn(v *KBDocumentDetail) string {
	return dsKey(v.KnowledgeBaseID, v.DataSourceID)
}

func resourcePolicyKeyFn(v *ResourcePolicy) string { return v.ResourceArn }

// registerAllTables registers every backend resource table (and its
// secondary indexes) exactly once. It must be called during construction
// only, immediately after b.registry is created -- store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// instead (see InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.agents = store.Register(b.registry, "agents", store.New(agentKeyFn))

	b.agentVersions = store.Register(b.registry, "agentVersions", store.New(agentVersionKeyFn))
	b.agentVersionsByAgent = b.agentVersions.AddIndex("byAgent", agentVersionByAgentKeyFn)

	b.actionGroups = store.Register(b.registry, "actionGroups", store.New(actionGroupKeyFn))
	b.actionGroupsByAgentVersion = b.actionGroups.AddIndex("byAgentVersion", actionGroupByAgentVersionKeyFn)

	b.agentAliases = store.Register(b.registry, "agentAliases", store.New(agentAliasKeyFn))
	b.agentAliasesByAgent = b.agentAliases.AddIndex("byAgent", agentAliasByAgentKeyFn)

	b.agentCollaborators = store.Register(b.registry, "agentCollaborators", store.New(agentCollaboratorKeyFn))
	b.agentCollaboratorsByAgentVersion = b.agentCollaborators.AddIndex(
		"byAgentVersion", agentCollaboratorByAgentVersionKeyFn,
	)

	b.knowledgeBases = store.Register(b.registry, "knowledgeBases", store.New(knowledgeBaseKeyFn))

	b.agentKBAssocs = store.Register(b.registry, "agentKBAssocs", store.New(agentKBKeyFn))
	b.agentKBAssocsByAgentVersion = b.agentKBAssocs.AddIndex("byAgentVersion", agentKBByAgentVersionKeyFn)

	b.dataSources = store.Register(b.registry, "dataSources", store.New(dataSourceKeyFn))
	b.dataSourcesByKB = b.dataSources.AddIndex("byKB", dataSourceByKBKeyFn)

	b.ingestionJobs = store.Register(b.registry, "ingestionJobs", store.New(ingestionJobKeyFn))
	b.ingestionJobsByDataSource = b.ingestionJobs.AddIndex("byDataSource", ingestionJobByDataSourceKeyFn)

	b.flows = store.Register(b.registry, "flows", store.New(flowKeyFn))

	b.flowVersions = store.Register(b.registry, "flowVersions", store.New(flowVersionKeyFn))
	b.flowVersionsByFlow = b.flowVersions.AddIndex("byFlow", flowVersionByFlowKeyFn)

	b.flowAliases = store.Register(b.registry, "flowAliases", store.New(flowAliasKeyFn))
	b.flowAliasesByFlow = b.flowAliases.AddIndex("byFlow", flowAliasByFlowKeyFn)

	b.prompts = store.Register(b.registry, "prompts", store.New(promptKeyFn))

	b.promptVersions = store.Register(b.registry, "promptVersions", store.New(promptVersionKeyFn))
	b.promptVersionsByPrompt = b.promptVersions.AddIndex("byPrompt", promptVersionByPromptKeyFn)

	b.kbDocuments = store.Register(b.registry, "kbDocuments", store.New(kbDocumentKeyFn))
	b.kbDocumentsByDataSource = b.kbDocuments.AddIndex("byDataSource", kbDocumentByDataSourceKeyFn)

	b.resourcePolicies = store.Register(b.registry, "resourcePolicies", store.New(resourcePolicyKeyFn))
}
