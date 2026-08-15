package securityhub

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is registered exactly once,
// here, as a *store.Table[T] on b.registry, following the pattern established
// by services/ec2 (commit 12e611a4, data-driven registration slice) and
// services/ses (commit e9af4cc7, identity-less DTOs). See pkgs/store's
// package doc for the underlying primitive.
//
// Every converted field's value type already carries its own identity field
// that matches the pre-conversion map key exactly (RuleArn, InsightArn,
// AccountId, ...), so every table below is "clean" -- registered directly on
// b.registry with no DTO layer, unlike services/sqs's pilot or ses's
// identity-less types.
//
// controlAssocOverrides was the one nested map (map[string]map[string]*T,
// keyed [standardsArn][securityControlID]); it is flattened here to a single
// store.Table[StandardsControlAssociation] keyed by the composite
// "<standardsArn>|<securityControlID>" (controlAssocOverrideKey). Both halves
// of that composite key already live on the value as its own
// StandardsArn/SecurityControlID fields, so -- as with every other table in
// this file -- no synthetic identity field is needed. No secondary index is
// added: every access site looks up (or writes) a single, fully-known
// composite key; nothing scans "all associations for standard X".
//
// The following resource fields are deliberately left as plain maps (not
// registered here) because their values are not *T -- store.Table requires a
// map[string]*V shape, and these are either map[string]map[string]any/string
// (nested, not keyed-by-identity-value) or map[string]string (plain string
// values):
//   - tags: map[string]map[string]string (resourceArn -> tagKey -> tagValue)
//   - findings: map[string]map[string]any (ASFF finding documents)
//   - findingHistory: map[string][]map[string]any (finding key -> ordered
//     FindingHistoryRecord-shaped change log, see recordFindingHistory)
//   - controlParams: map[string]map[string]any (securityControlID -> params)
//   - productSubscriptions: map[string]string (subscriptionArn -> productArn)
//   - orgAdminAccounts: map[string]string (accountID -> status)
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func automationRuleKeyFn(v *AutomationRule) string { return v.RuleArn }

func insightKeyFn(v *Insight) string { return v.InsightArn }

func actionTargetKeyFn(v *ActionTarget) string { return v.ActionTargetArn }

func memberKeyFn(v *Member) string { return v.AccountId }

func invitationKeyFn(v *Invitation) string { return v.InvitationId }

func configPolicyKeyFn(v *ConfigurationPolicy) string { return v.Id }

func configPolicyAssocKeyFn(v *ConfigurationPolicyAssociation) string { return v.TargetId }

func recommendedPolicyV2KeyFn(v *RecommendedPolicyV2) string { return v.MetadataUid }

func findingAggregatorKeyFn(v *FindingAggregator) string { return v.FindingAggregatorArn }

func controlOverrideKeyFn(v *StandardsControl) string { return v.StandardsControlArn }

func ticketV2KeyFn(v *TicketV2) string { return v.TicketId }

func connectorV2KeyFn(v *ConnectorV2) string { return v.ConnectorId }

func cspmConnectorKeyFn(v *CspmConnector) string { return v.ConnectorId }

func standardsSubscriptionKeyFn(v *StandardsSubscription) string { return v.StandardsSubscriptionArn }

func aggregatorV2KeyFn(v *AggregatorV2) string { return v.AggregatorV2Arn }

func automationRuleV2KeyFn(v *AutomationRuleV2) string { return v.Identifier }

// controlAssocOverrideKey builds the composite "<standardsArn>|<securityControlID>"
// key shared by every element of the flattened controlAssocOverrides table.
// Access sites use this directly so the exact same key is used for
// Get/Put/Has.
func controlAssocOverrideKey(standardsArn, securityControlID string) string {
	return standardsArn + "|" + securityControlID
}

func controlAssocOverrideKeyFn(v *StandardsControlAssociation) string {
	return controlAssocOverrideKey(v.StandardsArn, v.SecurityControlID)
}

// registerAllTables registers every converted resource map on b.registry
// exactly once. It must be called during construction only, immediately
// after b.registry is created -- store.Register panics on a duplicate name,
// so runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in backend.go), and Restore must register tables on
// a fresh backend (via NewInMemoryBackend) before calling
// b.registry.RestoreAll.
func registerAllTables(b *InMemoryBackend) {
	b.automationRules = store.Register(b.registry, "automationRules", store.New(automationRuleKeyFn))
	b.insights = store.Register(b.registry, "insights", store.New(insightKeyFn))
	b.actionTargets = store.Register(b.registry, "actionTargets", store.New(actionTargetKeyFn))
	b.members = store.Register(b.registry, "members", store.New(memberKeyFn))
	b.invitations = store.Register(b.registry, "invitations", store.New(invitationKeyFn))
	b.configPolicies = store.Register(b.registry, "configPolicies", store.New(configPolicyKeyFn))
	b.configPolicyAssocs = store.Register(b.registry, "configPolicyAssocs", store.New(configPolicyAssocKeyFn))
	b.recommendedPoliciesV2 = store.Register(
		b.registry,
		"recommendedPoliciesV2",
		store.New(recommendedPolicyV2KeyFn),
	)
	b.findingAggregators = store.Register(b.registry, "findingAggregators", store.New(findingAggregatorKeyFn))
	b.controlOverrides = store.Register(b.registry, "controlOverrides", store.New(controlOverrideKeyFn))
	b.controlAssocOverrides = store.Register(
		b.registry,
		"controlAssocOverrides",
		store.New(controlAssocOverrideKeyFn),
	)
	b.ticketsV2 = store.Register(b.registry, "ticketsV2", store.New(ticketV2KeyFn))
	b.connectorsV2 = store.Register(b.registry, "connectorsV2", store.New(connectorV2KeyFn))
	b.cspmConnectors = store.Register(b.registry, "cspmConnectors", store.New(cspmConnectorKeyFn))
	b.standardsSubscriptions = store.Register(
		b.registry,
		"standardsSubscriptions",
		store.New(standardsSubscriptionKeyFn),
	)
	b.aggregatorsV2 = store.Register(b.registry, "aggregatorsV2", store.New(aggregatorV2KeyFn))
	b.automationRulesV2 = store.Register(b.registry, "automationRulesV2", store.New(automationRuleV2KeyFn))
}
