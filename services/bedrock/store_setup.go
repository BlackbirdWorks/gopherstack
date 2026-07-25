package bedrock

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (commit 12e611a4) and services/sqs (commit 0f09d77c)
// pilots for the pattern this follows.
//
// At the time this file was written bedrock had no persistence.go (it was
// never wired into pkgs/persistence's Manager -- see cli.go, which only
// registers backends that implement persistence.Persistable), so this
// conversion was originally an internal storage-layer swap only: it
// eliminated the map init/Reset boilerplate but did not add Snapshot/Restore.
// See persistence.go for the Snapshot/Restore pair added on top of this
// conversion, which is what now drives every b.registry-registered table
// (including the four lazy per-parent kinds below, via
// lazyTableAccessorsByPrefix) through a real snapshot/restore cycle.
//
// flowVersions, promptVersions, agentVersions, and agentCollaborators were
// previously two-level maps (map[string]map[string]*T, parent ID -> child ID
// -> value). Each child value type carries its parent's ID as a field
// (FlowVersion.FlowID, PromptVersion.PromptID, AgentVersion.AgentID,
// AgentCollaborator.AgentID), so each parent's children are a pure-key
// store.Table; the outer map becomes map[string]*store.Table[T], one table
// per parent, registered lazily on first use -- exactly the per-region
// lazy-registration pattern services/kms uses for keys/aliases/grants (see
// keysStore/aliasesStore/grantsRegion in services/kms/backend.go).
//
// A handful of fields are deliberately NOT registered here and remain plain
// maps -- see the comment block above registerAllTables for the list and why.
import (
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// ---------------------------------------------------------------------------
// Key functions for flat (single-level) tables.
// ---------------------------------------------------------------------------

func guardrailsKeyFn(v *Guardrail) string { return v.GuardrailID }

// guardrailVersionsKeyFn mirrors the "guardrailID:version" key CreateGuardrailVersion builds.
func guardrailVersionsKeyFn(v *GuardrailVersion) string { return v.GuardrailID + ":" + v.Version }

func provisionedModelThroughputsKeyFn(v *ProvisionedModelThroughput) string {
	return v.ProvisionedModelArn
}

func evaluationJobsKeyFn(v *EvaluationJob) string { return v.JobArn }

func automatedReasoningPoliciesKeyFn(v *AutomatedReasoningPolicy) string { return v.PolicyArn }

func arpBuildWorkflowsKeyFn(v *AutomatedReasoningPolicyBuildWorkflow) string {
	return v.BuildWorkflowID
}

func arpTestCasesKeyFn(v *AutomatedReasoningPolicyTestCase) string { return v.TestCaseID }

// arpVersionsKeyFn mirrors the "policyARN:versionNum" key
// CreateAutomatedReasoningPolicyVersion builds. The value's own PolicyArn
// field holds the *versioned* ARN (policyARN + "/version/" + versionNum, see
// CreateAutomatedReasoningPolicyVersion), so the base policy ARN is recovered
// by trimming that suffix back off -- still a pure function of the value.
func arpVersionsKeyFn(v *AutomatedReasoningPolicyVersion) string {
	base := strings.TrimSuffix(v.PolicyArn, "/version/"+v.Version)

	return base + ":" + v.Version
}

func customModelsKeyFn(v *CustomModel) string                                { return v.ModelArn }
func customModelDeploymentsKeyFn(v *CustomModelDeployment) string            { return v.CustomModelDeploymentArn }
func foundationModelAgreementsKeyFn(v *FoundationModelAgreement) string      { return v.ModelID }
func modelCustomizationJobsKeyFn(v *ModelCustomizationJob) string            { return v.JobArn }
func modelCopyJobsKeyFn(v *ModelCopyJob) string                              { return v.JobArn }
func modelImportJobsKeyFn(v *ModelImportJob) string                          { return v.JobArn }
func inferenceProfilesKeyFn(v *InferenceProfile) string                      { return v.InferenceProfileArn }
func marketplaceEndpointsKeyFn(v *MarketplaceModelEndpoint) string           { return v.EndpointArn }
func modelInvocationJobsKeyFn(v *ModelInvocationJob) string                  { return v.JobArn }
func promptRoutersKeyFn(v *PromptRouter) string                              { return v.PromptRouterArn }
func enforcedGuardrailConfigsKeyFn(v *AccountEnforcedGuardrailConfig) string { return v.ConfigID }
func agentsKeyFn(v *Agent) string                                            { return v.AgentID }

func agentActionGroupsKeyFn(v *AgentActionGroup) string {
	return agentActionGroupKey(v.AgentID, v.ActionGroupID)
}

func agentAliasesKeyFn(v *AgentAlias) string { return agentAliasKey(v.AgentID, v.AgentAliasID) }

func agentKBAssociationsKeyFn(v *AgentKnowledgeBaseAssociation) string {
	return agentKBKey(v.AgentID, v.KnowledgeBaseID)
}

func knowledgeBasesKeyFn(v *KnowledgeBase) string { return v.KnowledgeBaseID }
func dataSourcesKeyFn(v *DataSource) string       { return v.KnowledgeBaseID + "/" + v.DataSourceID }

func ingestionJobsKeyFn(v *IngestionJob) string {
	return ingestionJobKey(v.KnowledgeBaseID, v.DataSourceID, v.IngestionJobID)
}

func flowsKeyFn(v *Flow) string            { return v.FlowID }
func flowAliasesKeyFn(v *FlowAlias) string { return flowAliasKey(v.FlowID, v.FlowAliasID) }
func promptsKeyFn(v *Prompt) string        { return v.PromptID }

func kbDocumentsKeyFn(v *KnowledgeBaseDocument) string {
	return kbDocKey(v.KnowledgeBaseID, v.DataSourceID, v.DocumentID)
}

// parity-4 key functions.
func advancedPromptOptimizationJobsKeyFn(v *AdvancedPromptOptimizationJob) string { return v.JobArn }
func resourcePoliciesKeyFn(v *ResourcePolicy) string                              { return v.ResourceArn }

// ---------------------------------------------------------------------------
// Key functions for the per-parent lazily-registered tables.
// ---------------------------------------------------------------------------

func flowVersionKeyFn(v *FlowVersion) string             { return v.Version }
func promptVersionKeyFn(v *PromptVersion) string         { return v.Version }
func agentVersionKeyFn(v *AgentVersion) string           { return v.AgentVersion }
func agentCollaboratorKeyFn(v *AgentCollaborator) string { return v.CollaboratorID }

// flowVersionsStore returns (registering lazily) the per-flow version table.
// Registration mutates b.registry and b.flowVersions, so this must only be
// called from a write-locked path (mirrors services/kms's keysStore).
func (b *InMemoryBackend) flowVersionsStore(flowID string) *store.Table[FlowVersion] {
	if t, ok := b.flowVersions[flowID]; ok {
		return t
	}

	t := store.Register(b.registry, "flowVersions:"+flowID, store.New(flowVersionKeyFn))
	b.flowVersions[flowID] = t

	return t
}

// promptVersionsStore returns (registering lazily) the per-prompt version table.
func (b *InMemoryBackend) promptVersionsStore(promptID string) *store.Table[PromptVersion] {
	if t, ok := b.promptVersions[promptID]; ok {
		return t
	}

	t := store.Register(b.registry, "promptVersions:"+promptID, store.New(promptVersionKeyFn))
	b.promptVersions[promptID] = t

	return t
}

// agentVersionsStore returns (registering lazily) the per-agent version table.
func (b *InMemoryBackend) agentVersionsStore(agentID string) *store.Table[AgentVersion] {
	if t, ok := b.agentVersions[agentID]; ok {
		return t
	}

	t := store.Register(b.registry, "agentVersions:"+agentID, store.New(agentVersionKeyFn))
	b.agentVersions[agentID] = t

	return t
}

// agentCollaboratorsStore returns (registering lazily) the per-agent collaborators table.
func (b *InMemoryBackend) agentCollaboratorsStore(agentID string) *store.Table[AgentCollaborator] {
	if t, ok := b.agentCollaborators[agentID]; ok {
		return t
	}

	t := store.Register(b.registry, "agentCollaborators:"+agentID, store.New(agentCollaboratorKeyFn))
	b.agentCollaborators[agentID] = t

	return t
}

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
//
// The following resource fields are deliberately left as plain maps (not
// registered here) because they are not a map[string]*T shape store.Table can
// replace:
//   - arpVersionCountByPolicy, flowVersionCounters, promptVersionCounters,
//     agentVersionCounters: map[string]int per-parent version counters, not
//     resource values.
//   - guardrailsByName, guardrailsByARN, pmtsByName, arpByName,
//     customModelsByName, customModelDeployByName, evaluationJobsByName,
//     customizationJobsByName, inferenceProfilesByName,
//     marketplaceEndpointsByName, promptRoutersByName, agentsByName,
//     kbByName, flowsByName, promptsByName: map[string]string secondary
//     name/ARN -> ID lookup indexes, not primary resource storage.
//   - agentTags: map[string]map[string]string tag map keyed externally by
//     resource ARN then tag key; the value (string) carries no identity
//     field of its own.
//   - agentMemory, arpAnnotations: map[string][]any; the value is a raw
//     slice with no identity field of its own to derive a store.Table key
//     function from.
//   - loggingConfig: a single *ModelInvocationLoggingConfiguration, not a map.
//   - accountDataRetention (parity-4): a single *AccountDataRetention, not a
//     map -- same shape as loggingConfig.
//   - foundationModels: a []*FoundationModelSummary slice (seeded once, not
//     keyed), not a map.
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// lazyTableAccessorsByPrefix maps each lazily-registered per-parent table
// kind's name prefix (see flowVersionsStore, promptVersionsStore,
// agentVersionsStore, agentCollaboratorsStore above -- each registers its
// table under "<prefix>:<parentID>" on first use) to a callback that forces
// that same lazy registration for a given parent ID. persistence.go's Restore
// uses this to pre-register every (prefix, parentID) pair present in an
// incoming snapshot's Tables blob before calling b.registry.RestoreAll --
// RestoreAll only restores tables already registered on b.registry (see
// [github.com/blackbirdworks/gopherstack/pkgs/store.Registry.RestoreAll]), so
// a parent ID a fresh backend has never touched would otherwise have its
// per-parent table silently dropped instead of restored. Mirrors
// services/ssm's tableAccessorsByPrefix (store_setup.go), adapted for a
// ":"-separated name instead of ssm's "/"-separated region suffix.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var lazyTableAccessorsByPrefix = map[string]func(b *InMemoryBackend, parentID string){
	"flowVersions":       func(b *InMemoryBackend, parentID string) { b.flowVersionsStore(parentID) },
	"promptVersions":     func(b *InMemoryBackend, parentID string) { b.promptVersionsStore(parentID) },
	"agentVersions":      func(b *InMemoryBackend, parentID string) { b.agentVersionsStore(parentID) },
	"agentCollaborators": func(b *InMemoryBackend, parentID string) { b.agentCollaboratorsStore(parentID) },
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.guardrails = store.Register(b.registry, "guardrails", store.New(guardrailsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.guardrailVersions = store.Register(b.registry, "guardrailVersions", store.New(guardrailVersionsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.provisionedModelThroughputs = store.Register(
			b.registry,
			"provisionedModelThroughputs",
			store.New(provisionedModelThroughputsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.evaluationJobs = store.Register(b.registry, "evaluationJobs", store.New(evaluationJobsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.automatedReasoningPolicies = store.Register(
			b.registry,
			"automatedReasoningPolicies",
			store.New(automatedReasoningPoliciesKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.arpBuildWorkflows = store.Register(b.registry, "arpBuildWorkflows", store.New(arpBuildWorkflowsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.arpTestCases = store.Register(b.registry, "arpTestCases", store.New(arpTestCasesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.arpVersions = store.Register(b.registry, "arpVersions", store.New(arpVersionsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.customModels = store.Register(b.registry, "customModels", store.New(customModelsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.customModelDeployments = store.Register(
			b.registry,
			"customModelDeployments",
			store.New(customModelDeploymentsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.foundationModelAgreements = store.Register(
			b.registry,
			"foundationModelAgreements",
			store.New(foundationModelAgreementsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.modelCustomizationJobs = store.Register(
			b.registry,
			"modelCustomizationJobs",
			store.New(modelCustomizationJobsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.modelCopyJobs = store.Register(b.registry, "modelCopyJobs", store.New(modelCopyJobsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.modelImportJobs = store.Register(b.registry, "modelImportJobs", store.New(modelImportJobsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.inferenceProfiles = store.Register(b.registry, "inferenceProfiles", store.New(inferenceProfilesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.marketplaceEndpoints = store.Register(
			b.registry,
			"marketplaceEndpoints",
			store.New(marketplaceEndpointsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.modelInvocationJobs = store.Register(b.registry, "modelInvocationJobs", store.New(modelInvocationJobsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.promptRouters = store.Register(b.registry, "promptRouters", store.New(promptRoutersKeyFn))
	},
	func(b *InMemoryBackend) {
		b.enforcedGuardrailConfigs = store.Register(
			b.registry,
			"enforcedGuardrailConfigs",
			store.New(enforcedGuardrailConfigsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.agents = store.Register(b.registry, "agents", store.New(agentsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.agentActionGroups = store.Register(b.registry, "agentActionGroups", store.New(agentActionGroupsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.agentAliases = store.Register(b.registry, "agentAliases", store.New(agentAliasesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.agentKBAssociations = store.Register(
			b.registry,
			"agentKBAssociations",
			store.New(agentKBAssociationsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.knowledgeBases = store.Register(b.registry, "knowledgeBases", store.New(knowledgeBasesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.dataSources = store.Register(b.registry, "dataSources", store.New(dataSourcesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.ingestionJobs = store.Register(b.registry, "ingestionJobs", store.New(ingestionJobsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.flows = store.Register(b.registry, "flows", store.New(flowsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.flowAliases = store.Register(b.registry, "flowAliases", store.New(flowAliasesKeyFn))
	},
	func(b *InMemoryBackend) {
		b.prompts = store.Register(b.registry, "prompts", store.New(promptsKeyFn))
	},
	func(b *InMemoryBackend) {
		b.kbDocuments = store.Register(b.registry, "kbDocuments", store.New(kbDocumentsKeyFn))
	},
	// parity-4 tables.
	func(b *InMemoryBackend) {
		b.advancedPromptOptimizationJobs = store.Register(
			b.registry,
			"advancedPromptOptimizationJobs",
			store.New(advancedPromptOptimizationJobsKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.resourcePolicies = store.Register(b.registry, "resourcePolicies", store.New(resourcePoliciesKeyFn))
	},
}
