package wafv2

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]map[string]*T resource field on InMemoryBackend is replaced with
// a *store.Table[T] (plus store.Index[T] secondary indexes reproducing the
// old map[region]map[arn|nameScope]string side maps), registered exactly
// once here. See pkgs/store's package doc and the pattern established by
// services/ec2 (commit 12e611a4, data-driven registration slice),
// services/sqs (commit 0f09d77c, DTO-registry pilot), and services/ses
// (commit e9af4cc7, identity-less json:"-" key fields).
//
// Every WAFv2 resource used to live in a map[region]map[key]*T: region was
// the outer partition (REGIONAL resources bucket under the request region,
// CLOUDFRONT resources always bucket under ""), and a same-named resource in
// two different regions was never a collision (see isolation_test.go).
// store.Table only supports a single string key, so that two-level nesting
// is flattened into one composite key via regionKey(region, innerKey) —
// this is a mechanical shape change only; every lookup below reconstructs
// exactly the same address the old nested map would have.
//
// Tables split into two groups:
//
//   - "Clean" tables (webACLs, ipSets, regexPatternSets, ruleGroups) key off
//     fields the value type already carries: ARN embeds the resource's
//     normalized storage region consistently at both create and delete time
//     (buildXxxARN/arnRegionForScope agree with storeRegion), so the
//     region+id composite key, the by-ARN index, and the by-name+scope index
//     are all pure functions of existing exported fields. No extra field was
//     needed, so these are registered directly on b.registry and
//     persistence.go drives them through b.registry.SnapshotAll()/
//     RestoreAll().
//   - "Dirty" tables (managedRuleSets, apiKeys) key off the RAW request
//     region (getRegion), which for CLOUDFRONT-scoped ManagedRuleSets and
//     APIKeys is NOT the same value ARN's arnRegionForScope-normalized
//     region segment holds (see ManagedRuleSet.Region/APIKey.Region for the
//     detailed asymmetry). Each gained a Region field tagged json:"-" purely
//     so store.Table's keyFn can derive a key from the value. They are NOT
//     registered on b.registry: persistence.go instead builds a throwaway
//     DTO store.Registry whose DTO types carry Region as a real JSON field,
//     so Snapshot/Restore never depends on a json:"-" field to round-trip.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func webACLPrimaryKey(w *WebACL) string { return regionKey(regionFromARN(w.ARN), w.ID) }
func webACLARNKey(w *WebACL) string     { return w.ARN }
func webACLRegionKey(w *WebACL) string  { return regionFromARN(w.ARN) }
func webACLNameScopeKey(w *WebACL) string {
	return regionKey(regionFromARN(w.ARN), nameScope(w.Name, w.Scope))
}

func ipSetPrimaryKey(s *IPSet) string { return regionKey(regionFromARN(s.ARN), s.ID) }
func ipSetARNKey(s *IPSet) string     { return s.ARN }
func ipSetRegionKey(s *IPSet) string  { return regionFromARN(s.ARN) }
func ipSetNameScopeKey(s *IPSet) string {
	return regionKey(regionFromARN(s.ARN), nameScope(s.Name, s.Scope))
}

func regexPatternSetPrimaryKey(r *RegexPatternSet) string {
	return regionKey(regionFromARN(r.ARN), r.ID)
}
func regexPatternSetARNKey(r *RegexPatternSet) string    { return r.ARN }
func regexPatternSetRegionKey(r *RegexPatternSet) string { return regionFromARN(r.ARN) }
func regexPatternSetNameScopeKey(r *RegexPatternSet) string {
	return regionKey(regionFromARN(r.ARN), nameScope(r.Name, r.Scope))
}

func ruleGroupPrimaryKey(rg *RuleGroup) string { return regionKey(regionFromARN(rg.ARN), rg.ID) }
func ruleGroupARNKey(rg *RuleGroup) string     { return rg.ARN }
func ruleGroupRegionKey(rg *RuleGroup) string  { return regionFromARN(rg.ARN) }
func ruleGroupNameScopeKey(rg *RuleGroup) string {
	return regionKey(regionFromARN(rg.ARN), nameScope(rg.Name, rg.Scope))
}

func managedRuleSetPrimaryKey(ms *ManagedRuleSet) string { return regionKey(ms.Region, ms.ID) }
func managedRuleSetRegionKey(ms *ManagedRuleSet) string  { return ms.Region }

func apiKeyPrimaryKey(a *APIKey) string {
	return regionKey(a.Region, apiKeyMapKey(a.Scope, a.APIKeyValue))
}
func apiKeyRegionKey(a *APIKey) string { return a.Region }

// registerAllTables constructs every store.Table-backed resource field
// exactly once, at construction time. "Clean" tables are registered on
// b.registry; "dirty" tables are constructed alongside them but deliberately
// NOT registered -- see the file doc comment above. It must be called during
// construction only, never on every Reset(): store.Register panics on a
// duplicate name, so runtime resets go through Reset (backend.go) instead,
// which calls b.registry.ResetAll() plus the dirty tables' own Reset().
func registerAllTables(b *InMemoryBackend) {
	b.webACLs = store.Register(b.registry, "webACLs", store.New(webACLPrimaryKey))
	b.webACLsByARN = b.webACLs.AddIndex("arn", webACLARNKey)
	b.webACLsByNameScope = b.webACLs.AddIndex("nameScope", webACLNameScopeKey)
	b.webACLsByRegion = b.webACLs.AddIndex("region", webACLRegionKey)

	b.ipSets = store.Register(b.registry, "ipSets", store.New(ipSetPrimaryKey))
	b.ipSetsByARN = b.ipSets.AddIndex("arn", ipSetARNKey)
	b.ipSetsByNameScope = b.ipSets.AddIndex("nameScope", ipSetNameScopeKey)
	b.ipSetsByRegion = b.ipSets.AddIndex("region", ipSetRegionKey)

	b.regexPatternSets = store.Register(b.registry, "regexPatternSets", store.New(regexPatternSetPrimaryKey))
	b.regexPatternSetsByARN = b.regexPatternSets.AddIndex("arn", regexPatternSetARNKey)
	b.regexPatternSetsByNameScope = b.regexPatternSets.AddIndex("nameScope", regexPatternSetNameScopeKey)
	b.regexPatternSetsByRegion = b.regexPatternSets.AddIndex("region", regexPatternSetRegionKey)

	b.ruleGroups = store.Register(b.registry, "ruleGroups", store.New(ruleGroupPrimaryKey))
	b.ruleGroupsByARN = b.ruleGroups.AddIndex("arn", ruleGroupARNKey)
	b.ruleGroupsByNameScope = b.ruleGroups.AddIndex("nameScope", ruleGroupNameScopeKey)
	b.ruleGroupsByRegion = b.ruleGroups.AddIndex("region", ruleGroupRegionKey)

	b.managedRuleSets = store.New(managedRuleSetPrimaryKey)
	b.managedRuleSetsByRegion = b.managedRuleSets.AddIndex("region", managedRuleSetRegionKey)

	b.apiKeys = store.New(apiKeyPrimaryKey)
	b.apiKeysByRegion = b.apiKeys.AddIndex("region", apiKeyRegionKey)
}

// resetTablesLocked resets every store.Table this backend owns -- both the
// "clean" ones on b.registry and the "dirty" ones that are not (they cannot
// be re-registered without panicking, so Reset drives them directly). Must
// be called with b.mu held for writing.
func (b *InMemoryBackend) resetTablesLocked() {
	b.registry.ResetAll()
	b.managedRuleSets.Reset()
	b.apiKeys.Reset()
}
