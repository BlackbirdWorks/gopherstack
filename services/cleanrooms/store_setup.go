package cleanrooms

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T, map[string]map[string]*T, and one
// map[string]map[string]map[string]*T resource collection on InMemoryBackend
// is registered exactly once, here, as a *store.Table[T]. See pkgs/store's
// package doc for the underlying primitive, and services/sesv2/
// services/codebuild/services/dax for the clean-direct-register template,
// and services/codeartifact's domains/repositories tables for the
// composite-key-but-still-direct-register template this file follows
// throughout.
//
// collaborations, memberships, and configuredTables were already flat,
// top-level map[string]*T collections keyed by their own real identity
// field (CollaborationIdentifier, MembershipIdentifier,
// ConfiguredTableIdentifier), so each registers directly with no composite
// key.
//
// ctAnalysisRules, ctAssociations, ctaAnalysisRules, analysisTemplates,
// privacyBudgetTemplates, idMappingTables, idNamespaceAssociations,
// camaAssociations, changeRequests, schemas, protectedQueries, and
// protectedJobs were previously nested two levels deep (outer key = a
// parent identifier -- configuredTableID, membershipID, or
// collaborationID; inner key = the resource's own identifier or, for the
// two AnalysisRule maps, its Type). Each becomes a flat *store.Table[T]
// keyed by a composite "parent|id" string (see the *Key helpers in
// backend.go), with a companion *store.Index grouping entries by the
// parent for the per-parent scans (List operations, and the
// DeleteConfiguredTable / DeleteConfiguredTableAssociation cascade
// deletes) the nested maps used to answer directly.
//
// schemaAnalysisRules was nested three levels deep (collaborationID ->
// name -> ruleType). It flattens to one *store.Table[SchemaAnalysisRule]
// keyed by the composite "collaborationID|name|ruleType" string
// (schemaAnalysisRuleKey in backend.go). No secondary index is needed:
// every access (GetSchemaAnalysisRule, BatchGetSchemaAnalysisRule) is a
// direct composite-key lookup, never a "list all rules" scan.
//
// Every value type above already carries the real, wire-visible field(s)
// its composite key is built from (e.g. ConfiguredTableAnalysisRule
// carries both ConfiguredTableIdentifier and Type; AnalysisTemplate
// carries both MembershipIdentifier and its own ID), so plain JSON
// marshaling loses nothing and every table here registers directly on
// b.registry -- unlike services/workmail or services/codeartifact's
// packageGroups/packages/etc, none of these needed a hidden unexported
// field or a DTO wrapper.
import (
	"github.com/blackbirdworks/gopherstack/pkgs/store"
)

// Table key functions below intentionally read the *correctly AWS-wire-keyed*
// sibling field (ID/CollaborationID/MembershipID/...), never the legacy
// "*Identifier" fields -- those are now internal-only (json:"-", see
// models.go) since their wire presence was invented, and a field tagged
// json:"-" does not round-trip through store.Registry's JSON-based
// Snapshot/Restore. Both fields hold the same value (set together at
// creation time), so this is a pure persistence-safety fix, not a behavior
// change.
func collaborationTableKeyFn(v *Collaboration) string { return v.ID }

func membershipTableKeyFn(v *Membership) string { return v.ID }

func configuredTableTableKeyFn(v *ConfiguredTable) string { return v.ID }

func ctAnalysisRuleTableKeyFn(v *ConfiguredTableAnalysisRule) string {
	return ctAnalysisRuleKey(v.ConfiguredTableID, v.Type)
}

func ctAnalysisRuleTableIndexKeyFn(v *ConfiguredTableAnalysisRule) string {
	return v.ConfiguredTableID
}

func ctAssociationTableKeyFn(v *ConfiguredTableAssociation) string {
	return membershipKey(v.MembershipID, v.ID)
}

func ctAssociationTableIndexKeyFn(v *ConfiguredTableAssociation) string {
	return v.MembershipID
}

func ctaAnalysisRuleTableKeyFn(v *ConfiguredTableAssociationAnalysisRule) string {
	return ctaAnalysisRuleKey(v.ConfiguredTableAssociationIdentifier, v.Type)
}

func ctaAnalysisRuleTableIndexKeyFn(v *ConfiguredTableAssociationAnalysisRule) string {
	return v.ConfiguredTableAssociationIdentifier
}

func analysisTemplateTableKeyFn(v *AnalysisTemplate) string {
	return membershipKey(v.MembershipID, v.ID)
}

func analysisTemplateTableIndexKeyFn(v *AnalysisTemplate) string { return v.MembershipID }

func privacyBudgetTemplateTableKeyFn(v *PrivacyBudgetTemplate) string {
	return membershipKey(v.MembershipID, v.ID)
}

func privacyBudgetTemplateTableIndexKeyFn(v *PrivacyBudgetTemplate) string {
	return v.MembershipID
}

func idMappingTableTableKeyFn(v *IDMappingTable) string {
	return membershipKey(v.MembershipID, v.ID)
}

func idMappingTableTableIndexKeyFn(v *IDMappingTable) string { return v.MembershipID }

func idNamespaceAssociationTableKeyFn(v *IDNamespaceAssociation) string {
	return membershipKey(v.MembershipID, v.ID)
}

func idNamespaceAssociationTableIndexKeyFn(v *IDNamespaceAssociation) string {
	return v.MembershipID
}

func camaAssociationTableKeyFn(v *ConfiguredAudienceModelAssociation) string {
	return membershipKey(v.MembershipID, v.ID)
}

func camaAssociationTableIndexKeyFn(v *ConfiguredAudienceModelAssociation) string {
	return v.MembershipID
}

func changeRequestTableKeyFn(v *CollaborationChangeRequest) string {
	return collaborationKey(v.CollaborationID, v.ID)
}

func changeRequestTableIndexKeyFn(v *CollaborationChangeRequest) string {
	return v.CollaborationID
}

func schemaTableKeyFn(v *Schema) string { return collaborationKey(v.CollaborationID, v.Name) }

func schemaTableIndexKeyFn(v *Schema) string { return v.CollaborationID }

func schemaAnalysisRuleTableKeyFn(v *SchemaAnalysisRule) string {
	return schemaAnalysisRuleKey(v.CollaborationID, v.Name, v.Type)
}

func protectedQueryTableKeyFn(v *ProtectedQuery) string {
	return membershipKey(v.MembershipID, v.ID)
}

func protectedQueryTableIndexKeyFn(v *ProtectedQuery) string { return v.MembershipID }

func protectedJobTableKeyFn(v *ProtectedJob) string {
	return membershipKey(v.MembershipID, v.ID)
}

func protectedJobTableIndexKeyFn(v *ProtectedJob) string { return v.MembershipID }

// registerAllTables registers every converted resource collection on
// b.registry exactly once. It must be called during construction only
// (immediately after b.registry is created -- see NewInMemoryBackend),
// never on every Reset() -- store.Register panics on a duplicate name, so
// runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	registerTopLevelTables(b)
	registerMembershipNestedTables(b)
	registerCollaborationNestedTables(b)
}

// registerTopLevelTables registers the three top-level collections plus the
// two type-keyed-under-a-single-parent collections (ctAnalysisRules,
// ctaAnalysisRules), factored out of registerAllTables to keep it small.
func registerTopLevelTables(b *InMemoryBackend) {
	b.collaborations = store.Register(b.registry, "collaborations", store.New(collaborationTableKeyFn))
	b.memberships = store.Register(b.registry, "memberships", store.New(membershipTableKeyFn))
	b.configuredTables = store.Register(b.registry, "configuredTables", store.New(configuredTableTableKeyFn))

	b.ctAnalysisRules = store.Register(b.registry, "ctAnalysisRules", store.New(ctAnalysisRuleTableKeyFn))
	b.ctAnalysisRulesByTable = b.ctAnalysisRules.AddIndex("byTable", ctAnalysisRuleTableIndexKeyFn)

	b.ctaAnalysisRules = store.Register(b.registry, "ctaAnalysisRules", store.New(ctaAnalysisRuleTableKeyFn))
	b.ctaAnalysisRulesByAssociation = b.ctaAnalysisRules.AddIndex("byAssociation", ctaAnalysisRuleTableIndexKeyFn)
}

// registerMembershipNestedTables registers every collection composite-keyed
// by membershipID, factored out of registerAllTables to keep it small.
func registerMembershipNestedTables(b *InMemoryBackend) {
	b.ctAssociations = store.Register(b.registry, "ctAssociations", store.New(ctAssociationTableKeyFn))
	b.ctAssociationsByMembership = b.ctAssociations.AddIndex("byMembership", ctAssociationTableIndexKeyFn)

	b.analysisTemplates = store.Register(b.registry, "analysisTemplates", store.New(analysisTemplateTableKeyFn))
	b.analysisTemplatesByMembership = b.analysisTemplates.AddIndex("byMembership", analysisTemplateTableIndexKeyFn)

	b.privacyBudgetTemplates = store.Register(
		b.registry, "privacyBudgetTemplates", store.New(privacyBudgetTemplateTableKeyFn),
	)
	b.privacyBudgetTemplatesByMembership = b.privacyBudgetTemplates.AddIndex(
		"byMembership", privacyBudgetTemplateTableIndexKeyFn,
	)

	b.idMappingTables = store.Register(b.registry, "idMappingTables", store.New(idMappingTableTableKeyFn))
	b.idMappingTablesByMembership = b.idMappingTables.AddIndex("byMembership", idMappingTableTableIndexKeyFn)

	b.idNamespaceAssociations = store.Register(
		b.registry, "idNamespaceAssociations", store.New(idNamespaceAssociationTableKeyFn),
	)
	b.idNamespaceAssociationsByMembership = b.idNamespaceAssociations.AddIndex(
		"byMembership", idNamespaceAssociationTableIndexKeyFn,
	)

	b.camaAssociations = store.Register(b.registry, "camaAssociations", store.New(camaAssociationTableKeyFn))
	b.camaAssociationsByMembership = b.camaAssociations.AddIndex("byMembership", camaAssociationTableIndexKeyFn)

	b.protectedQueries = store.Register(b.registry, "protectedQueries", store.New(protectedQueryTableKeyFn))
	b.protectedQueriesByMembership = b.protectedQueries.AddIndex("byMembership", protectedQueryTableIndexKeyFn)

	b.protectedJobs = store.Register(b.registry, "protectedJobs", store.New(protectedJobTableKeyFn))
	b.protectedJobsByMembership = b.protectedJobs.AddIndex("byMembership", protectedJobTableIndexKeyFn)
}

// registerCollaborationNestedTables registers every collection
// composite-keyed by collaborationID, plus schemaAnalysisRules (keyed by
// collaborationID|name|ruleType, no index needed -- see the file doc
// comment), factored out of registerAllTables to keep it small.
func registerCollaborationNestedTables(b *InMemoryBackend) {
	b.changeRequests = store.Register(b.registry, "changeRequests", store.New(changeRequestTableKeyFn))
	b.changeRequestsByCollaboration = b.changeRequests.AddIndex("byCollaboration", changeRequestTableIndexKeyFn)

	b.schemas = store.Register(b.registry, "schemas", store.New(schemaTableKeyFn))
	b.schemasByCollaboration = b.schemas.AddIndex("byCollaboration", schemaTableIndexKeyFn)

	b.schemaAnalysisRules = store.Register(
		b.registry, "schemaAnalysisRules", store.New(schemaAnalysisRuleTableKeyFn),
	)
}
