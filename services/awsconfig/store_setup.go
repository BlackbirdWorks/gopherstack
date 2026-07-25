package awsconfig

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry.
//
// Every identity used below is a real, wire-visible field already present on
// the value type (or, for StoredEvaluation, a field added purely for internal
// bookkeeping -- see its own doc comment in evaluation.go -- since that type
// is never itself returned on the wire). None of the tables below need
// codecommit's "dirty"/DTO treatment: unlike CodeCommit's File/PullRequestApprovalRule,
// nothing here needs a hidden json:"-" identity field, so every table is
// registered directly on b.registry and rides Registry.SnapshotAll/RestoreAll
// with no ephemeral DTO registry required.
//
// Two collections were previously nested maps (map[string]map[string]*T):
//
//   - resourceConfigs (type -> id -> *ResourceConfigItem) flattens to one
//     table keyed by "<resourceType>|<resourceID>" (resourceConfigItemKeyFn),
//     with a "byType" [store.Index] answering the old outer-map lookup
//     ("all resources of type X", used by ListDiscoveredResources).
//   - ruleResourceEvals (ruleName -> resourceKey -> *StoredEvaluation)
//     flattens to one table keyed by "<ruleName>|<resourceType>\x1f<resourceID>"
//     (storedEvaluationKeyFn), with two indexes: "byRule" (all evaluations
//     for a rule) and "byResource" (all evaluations for a resource across
//     every rule) replacing the two access patterns the nested map used to
//     answer directly.
//
// This mirrors the services/codecommit conversion (branches/commits nested
// under repositoryName, flattened to "<repositoryName>|<childID>" + a "byRepo"
// index) applied here to AWS Config's two nested collections.
//
// The remaining map fields on InMemoryBackend (ruleEvaluations, resourceHistory,
// resourceTags, remediationExceptions, customRulePolicies, orgCustomRulePolicies)
// hold a scalar or slice value with no identity of its own for store.Table to
// key on, so they are deliberately left as plain maps -- see the field
// comments on InMemoryBackend in store.go and persistence.go's doc comment
// for the persistence audit of each.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func recorderKeyFn(v *ConfigurationRecorder) string { return v.Name }

func serviceLinkedRecorderKeyFn(v *ServiceLinkedRecorderLink) string { return v.ServicePrincipal }

func channelKeyFn(v *DeliveryChannel) string { return v.Name }

// aggregationAuthKeyFn reuses aggregationAuthKey (aggregators.go), the same
// composite-key function PutAggregationAuthorization/DeleteAggregationAuthorization
// use, so Put/Get/Delete/Has all agree on the same key.
func aggregationAuthKeyFn(v *AggregationAuthorization) string {
	return aggregationAuthKey(v.AuthorizedAccountID, v.AuthorizedAwsRegion)
}

func configRuleKeyFn(v *ConfigRule) string { return v.ConfigRuleName }

func aggregatorKeyFn(v *ConfigurationAggregator) string { return v.ConfigurationAggregatorName }

func conformancePackKeyFn(v *ConformancePack) string { return v.ConformancePackName }

// conformancePackRuleLinkKey/conformancePackRuleLinkKeyFn/
// conformancePackRuleLinkPackIndexKeyFn build the composite store.Table
// primary key ("packName|ruleName") and the "byPack" secondary index key for
// ConformancePackRuleLink, mirroring storedEvaluationKeyFn's composite-key
// pattern above.
func conformancePackRuleLinkKey(packName, ruleName string) string {
	return packName + "|" + ruleName
}

func conformancePackRuleLinkKeyFn(v *ConformancePackRuleLink) string {
	return conformancePackRuleLinkKey(v.ConformancePackName, v.ConfigRuleName)
}

func conformancePackRuleLinkPackIndexKeyFn(v *ConformancePackRuleLink) string {
	return v.ConformancePackName
}

func orgConfigRuleKeyFn(v *OrganizationConfigRule) string { return v.OrganizationConfigRuleName }

func orgConformancePackKeyFn(v *OrganizationConformancePack) string {
	return v.OrganizationConformancePackName
}

func storedQueryKeyFn(v *StoredQuery) string { return v.QueryName }

func retentionConfigKeyFn(v *RetentionConfiguration) string { return v.Name }

func remediationConfigKeyFn(v *RemediationConfiguration) string { return v.ConfigRuleName }

// remediationExecutionKey/remediationExecutionKeyFn/remediationExecutionRuleIndexKeyFn
// build the composite store.Table primary key
// ("ruleName|resourceType\x1fresourceID", reusing resourceEvalKey's
// \x1f-separated resource half, see evaluation.go) and the "byRule" secondary
// index key for RemediationExecutionStatusEntry.
func remediationExecutionKey(ruleName, resourceType, resourceID string) string {
	return ruleName + "|" + resourceEvalKey(resourceType, resourceID)
}

func remediationExecutionKeyFn(v *RemediationExecutionStatusEntry) string {
	return remediationExecutionKey(v.RuleName, v.ResourceKey.ResourceType, v.ResourceKey.ResourceID)
}

func remediationExecutionRuleIndexKeyFn(v *RemediationExecutionStatusEntry) string { return v.RuleName }

func resourceEvaluationKeyFn(v *ResourceEvaluation) string { return v.ResourceEvaluationID }

// resourceConfigItemKey/resourceConfigItemKeyFn/resourceConfigItemTypeIndexKeyFn
// build the composite store.Table primary key ("resourceType|resourceID")
// shared by every access site that used to index into the nested
// map[string]map[string]*ResourceConfigItem.
func resourceConfigItemKey(resourceType, resourceID string) string {
	return resourceType + "|" + resourceID
}

func resourceConfigItemKeyFn(v *ResourceConfigItem) string {
	return resourceConfigItemKey(v.ResourceType, v.ResourceID)
}

func resourceConfigItemTypeIndexKeyFn(v *ResourceConfigItem) string { return v.ResourceType }

// storedEvaluationKeyFn/storedEvaluationRuleIndexKeyFn/storedEvaluationResourceIndexKeyFn
// build the composite store.Table primary key ("ruleName|resourceType\x1fresourceID",
// reusing resourceEvalKey's \x1f-separated resource half, see evaluation.go)
// and the two secondary index keys that replace the nested
// map[string]map[string]*StoredEvaluation's two access patterns.
func storedEvaluationKeyFn(v *StoredEvaluation) string {
	return v.RuleName + "|" + resourceEvalKey(v.ResourceType, v.ResourceID)
}

func storedEvaluationRuleIndexKeyFn(v *StoredEvaluation) string { return v.RuleName }

func storedEvaluationResourceIndexKeyFn(v *StoredEvaluation) string {
	return resourceEvalKey(v.ResourceType, v.ResourceID)
}

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created), never on every Reset() --
// store.Register panics on a duplicate name, so runtime resets go through
// b.registry.ResetAll() instead (see InMemoryBackend.Reset in store.go).
func registerAllTables(b *InMemoryBackend) {
	for _, register := range tableRegistrations {
		register(b)
	}
}

// tableRegistrations is the data-driven list registerAllTables walks: one
// closure per resource table, each binding its own store.New/store.Register
// call (and, for the two composite-keyed collections, their companion
// store.Table.AddIndex call) to the concrete field and value type.
//
//nolint:gochecknoglobals // registration table, analogous to errCodeLookup-style lookup tables elsewhere
var tableRegistrations = []func(*InMemoryBackend){
	func(b *InMemoryBackend) {
		b.recorders = store.Register(b.registry, "recorders", store.New(recorderKeyFn))
	},
	func(b *InMemoryBackend) {
		b.serviceLinkedRecorders = store.Register(
			b.registry, "serviceLinkedRecorders", store.New(serviceLinkedRecorderKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.channels = store.Register(b.registry, "channels", store.New(channelKeyFn))
	},
	func(b *InMemoryBackend) {
		b.aggregationAuths = store.Register(b.registry, "aggregationAuths", store.New(aggregationAuthKeyFn))
	},
	func(b *InMemoryBackend) {
		b.configRules = store.Register(b.registry, "configRules", store.New(configRuleKeyFn))
	},
	func(b *InMemoryBackend) {
		b.aggregators = store.Register(b.registry, "aggregators", store.New(aggregatorKeyFn))
	},
	func(b *InMemoryBackend) {
		b.conformancePacks = store.Register(b.registry, "conformancePacks", store.New(conformancePackKeyFn))
	},
	func(b *InMemoryBackend) {
		b.conformancePackRules = store.Register(
			b.registry, "conformancePackRules", store.New(conformancePackRuleLinkKeyFn),
		)
		b.conformancePackRulesByPack = b.conformancePackRules.AddIndex(
			"byPack", conformancePackRuleLinkPackIndexKeyFn,
		)
	},
	func(b *InMemoryBackend) {
		b.orgConfigRules = store.Register(b.registry, "orgConfigRules", store.New(orgConfigRuleKeyFn))
	},
	func(b *InMemoryBackend) {
		b.orgConformancePacks = store.Register(
			b.registry, "orgConformancePacks", store.New(orgConformancePackKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.storedQueries = store.Register(b.registry, "storedQueries", store.New(storedQueryKeyFn))
	},
	func(b *InMemoryBackend) {
		b.retentionConfigs = store.Register(b.registry, "retentionConfigs", store.New(retentionConfigKeyFn))
	},
	func(b *InMemoryBackend) {
		b.remediationConfigs = store.Register(b.registry, "remediationConfigs", store.New(remediationConfigKeyFn))
	},
	func(b *InMemoryBackend) {
		b.remediationExecutions = store.Register(
			b.registry, "remediationExecutions", store.New(remediationExecutionKeyFn),
		)
		b.remediationExecutionsByRule = b.remediationExecutions.AddIndex("byRule", remediationExecutionRuleIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.resourceEvaluations = store.Register(
			b.registry, "resourceEvaluations", store.New(resourceEvaluationKeyFn),
		)
	},
	func(b *InMemoryBackend) {
		b.resourceConfigs = store.Register(b.registry, "resourceConfigs", store.New(resourceConfigItemKeyFn))
		b.resourceConfigsByType = b.resourceConfigs.AddIndex("byType", resourceConfigItemTypeIndexKeyFn)
	},
	func(b *InMemoryBackend) {
		b.ruleResourceEvals = store.Register(b.registry, "ruleResourceEvals", store.New(storedEvaluationKeyFn))
		b.ruleResourceEvalsByRule = b.ruleResourceEvals.AddIndex("byRule", storedEvaluationRuleIndexKeyFn)
		b.ruleResourceEvalsByResource = b.ruleResourceEvals.AddIndex("byResource", storedEvaluationResourceIndexKeyFn)
	},
}
