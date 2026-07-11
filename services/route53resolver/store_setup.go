package route53resolver

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]map[string]*T resource field on InMemoryBackend (region -> id ->
// value) is replaced with a single flattened *store.Table[T] keyed by
// regionalKey(region, id), plus a secondary store.Index grouping by region --
// the same role the outer map[region] level used to play for the region-scoped
// List* operations. See pkgs/store's package doc and the services/ec2 (commit
// 12e611a4, data-driven registration slice) and services/ses (commit
// e9af4cc7, region field added to otherwise-identity-carrying types) pilots
// this follows.
//
// All 13 converted types already carry (or, for the 11 that didn't, now
// carry -- see backend.go's Region field doc comments) their own identity
// field (ID, or ResourceID for the three VPC-config types) plus a real,
// normally-serialized Region field, so every table here is "clean": the key
// function is a pure function of the value's own fields and each table is
// registered directly on b.registry, with no DTO indirection needed (unlike
// services/sqs, whose Queue/moveTaskState carry live, non-serializable
// fields -- route53resolver's types do not).
//
// The following resource fields are deliberately left as plain
// map[string]map[string]V (not registered here, not store.Table) because
// their value type is not a *T:
//   - tags: map[string]map[string][]svcTags.KV -- value is a tag-KV slice
//   - firewallRuleGroupPolicies, queryLogConfigPolicies, resolverRulePolicies:
//     map[string]map[string]string -- value is a bare policy-document string
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func endpointKeyFn(v *ResolverEndpoint) string    { return regionalKey(v.Region, v.ID) }
func endpointRegionFn(v *ResolverEndpoint) string { return v.Region }

func ruleKeyFn(v *ResolverRule) string    { return regionalKey(v.Region, v.ID) }
func ruleRegionFn(v *ResolverRule) string { return v.Region }

func firewallRuleGroupKeyFn(v *FirewallRuleGroup) string    { return regionalKey(v.Region, v.ID) }
func firewallRuleGroupRegionFn(v *FirewallRuleGroup) string { return v.Region }

func firewallRuleGroupAssociationKeyFn(v *FirewallRuleGroupAssociation) string {
	return regionalKey(v.Region, v.ID)
}
func firewallRuleGroupAssociationRegionFn(v *FirewallRuleGroupAssociation) string { return v.Region }

func firewallDomainListKeyFn(v *FirewallDomainList) string    { return regionalKey(v.Region, v.ID) }
func firewallDomainListRegionFn(v *FirewallDomainList) string { return v.Region }

func firewallRuleKeyFn(v *FirewallRule) string    { return regionalKey(v.Region, v.ID) }
func firewallRuleRegionFn(v *FirewallRule) string { return v.Region }

func outpostResolverKeyFn(v *OutpostResolver) string    { return regionalKey(v.Region, v.ID) }
func outpostResolverRegionFn(v *OutpostResolver) string { return v.Region }

func queryLogConfigKeyFn(v *ResolverQueryLogConfig) string    { return regionalKey(v.Region, v.ID) }
func queryLogConfigRegionFn(v *ResolverQueryLogConfig) string { return v.Region }

func queryLogConfigAssociationKeyFn(v *ResolverQueryLogConfigAssociation) string {
	return regionalKey(v.Region, v.ID)
}
func queryLogConfigAssociationRegionFn(v *ResolverQueryLogConfigAssociation) string { return v.Region }

func ruleAssociationKeyFn(v *ResolverRuleAssociation) string    { return regionalKey(v.Region, v.ID) }
func ruleAssociationRegionFn(v *ResolverRuleAssociation) string { return v.Region }

// firewallConfigKeyFn, resolverConfigKeyFn, and resolverDnssecConfigKeyFn key
// off ResourceID (the caller-supplied VPC ID), not ID (a backend-generated
// identifier) -- this mirrors the pre-conversion map[region]map[resourceID]*T
// nesting exactly (see e.g. GetFirewallConfig in backend.go).
func firewallConfigKeyFn(v *FirewallConfig) string    { return regionalKey(v.Region, v.ResourceID) }
func firewallConfigRegionFn(v *FirewallConfig) string { return v.Region }

func resolverConfigKeyFn(v *ResolverConfig) string    { return regionalKey(v.Region, v.ResourceID) }
func resolverConfigRegionFn(v *ResolverConfig) string { return v.Region }

func resolverDnssecConfigKeyFn(v *ResolverDnssecConfig) string {
	return regionalKey(v.Region, v.ResourceID)
}
func resolverDnssecConfigRegionFn(v *ResolverDnssecConfig) string { return v.Region }

// registerAllTables constructs every store.Table-backed resource field and
// its "byRegion" store.Index exactly once, at construction time. It must be
// called during construction only (immediately after b.registry is created),
// never on every Reset() -- store.Register panics on a duplicate name, so
// runtime resets go through b.registry.ResetAll() instead (see
// InMemoryBackend.Reset in backend.go).
func registerAllTables(b *InMemoryBackend) {
	b.endpoints = store.Register(b.registry, "endpoints", store.New(endpointKeyFn))
	b.endpointsByRegion = b.endpoints.AddIndex("byRegion", endpointRegionFn)

	b.rules = store.Register(b.registry, "rules", store.New(ruleKeyFn))
	b.rulesByRegion = b.rules.AddIndex("byRegion", ruleRegionFn)

	b.firewallRuleGroups = store.Register(b.registry, "firewallRuleGroups", store.New(firewallRuleGroupKeyFn))
	b.firewallRuleGroupsByRegion = b.firewallRuleGroups.AddIndex("byRegion", firewallRuleGroupRegionFn)

	b.firewallRuleGroupAssociations = store.Register(
		b.registry,
		"firewallRuleGroupAssociations",
		store.New(firewallRuleGroupAssociationKeyFn),
	)
	b.firewallRuleGroupAssociationsByRegion = b.firewallRuleGroupAssociations.AddIndex(
		"byRegion",
		firewallRuleGroupAssociationRegionFn,
	)

	b.firewallDomainLists = store.Register(b.registry, "firewallDomainLists", store.New(firewallDomainListKeyFn))
	b.firewallDomainListsByRegion = b.firewallDomainLists.AddIndex("byRegion", firewallDomainListRegionFn)

	b.firewallRules = store.Register(b.registry, "firewallRules", store.New(firewallRuleKeyFn))
	b.firewallRulesByRegion = b.firewallRules.AddIndex("byRegion", firewallRuleRegionFn)

	b.outpostResolvers = store.Register(b.registry, "outpostResolvers", store.New(outpostResolverKeyFn))
	b.outpostResolversByRegion = b.outpostResolvers.AddIndex("byRegion", outpostResolverRegionFn)

	b.queryLogConfigs = store.Register(b.registry, "queryLogConfigs", store.New(queryLogConfigKeyFn))
	b.queryLogConfigsByRegion = b.queryLogConfigs.AddIndex("byRegion", queryLogConfigRegionFn)

	b.queryLogConfigAssociations = store.Register(
		b.registry,
		"queryLogConfigAssociations",
		store.New(queryLogConfigAssociationKeyFn),
	)
	b.queryLogConfigAssociationsByRegion = b.queryLogConfigAssociations.AddIndex(
		"byRegion",
		queryLogConfigAssociationRegionFn,
	)

	b.ruleAssociations = store.Register(b.registry, "ruleAssociations", store.New(ruleAssociationKeyFn))
	b.ruleAssociationsByRegion = b.ruleAssociations.AddIndex("byRegion", ruleAssociationRegionFn)

	b.firewallConfigs = store.Register(b.registry, "firewallConfigs", store.New(firewallConfigKeyFn))
	b.firewallConfigsByRegion = b.firewallConfigs.AddIndex("byRegion", firewallConfigRegionFn)

	b.resolverConfigs = store.Register(b.registry, "resolverConfigs", store.New(resolverConfigKeyFn))
	b.resolverConfigsByRegion = b.resolverConfigs.AddIndex("byRegion", resolverConfigRegionFn)

	b.resolverDnssecConfigs = store.Register(b.registry, "resolverDnssecConfigs", store.New(resolverDnssecConfigKeyFn))
	b.resolverDnssecConfigsByRegion = b.resolverDnssecConfigs.AddIndex("byRegion", resolverDnssecConfigRegionFn)
}
