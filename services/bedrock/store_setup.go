package bedrock

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry. See pkgs/store's package doc and
// the services/ec2 (commit 12e611a4) and services/sqs (commit 0f09d77c)
// pilots for the pattern this follows.
//
// See persistence.go for the Snapshot/Restore pair added on top of this
// conversion, which drives every b.registry-registered table through a real
// snapshot/restore cycle.
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

// parity-4 key functions.
func advancedPromptOptimizationJobsKeyFn(v *AdvancedPromptOptimizationJob) string { return v.JobArn }
func resourcePoliciesKeyFn(v *ResourcePolicy) string                              { return v.ResourceArn }

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only (immediately after
// b.registry is created), never on every Reset() -- store.Register panics on a
// duplicate name, so runtime resets go through registry.ResetAll() instead
// (see InMemoryBackend.Reset in backend.go).
//
// The following resource fields are deliberately left as plain maps (not
// registered here) because they are not a map[string]*T shape store.Table can
// replace:
//   - arpVersionCountByPolicy: map[string]int per-parent version counter, not
//     a resource value.
//   - guardrailsByName, guardrailsByARN, pmtsByName, arpByName,
//     customModelsByName, customModelDeployByName, evaluationJobsByName,
//     customizationJobsByName, inferenceProfilesByName,
//     marketplaceEndpointsByName, promptRoutersByName: map[string]string
//     secondary name/ARN -> ID lookup indexes, not primary resource storage.
//   - arpAnnotations: map[string][]any; the value is a raw slice with no
//     identity field of its own to derive a store.Table key function from.
//   - arpAnnotationSetHash: map[string]string optimistic-concurrency token,
//     same key shape as arpAnnotations and deliberately not persisted (see
//     its field doc comment in this struct).
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
