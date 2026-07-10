package waf

// Code in this file supports Phase 3.3 of the datalayer refactor: every
// map[string]*T resource field on InMemoryBackend is replaced with a
// *store.Table[T], following the pattern established by services/sesv2 and
// services/dax. See pkgs/store's package doc for the underlying primitive.
//
// Every convertible value type here already carries its own identity as a
// real (non-json:"-") field -- WebACL.WebACLId, Rule.RuleId,
// RateBasedRule.RuleId, IPSet.IPSetId, ByteMatchSet.ByteMatchSetId,
// SizeConstraintSet.SizeConstraintSetId,
// SqlInjectionMatchSet.SqlInjectionMatchSetId, XssMatchSet.XssMatchSetId,
// GeoMatchSet.GeoMatchSetId, RegexPatternSet.RegexPatternSetId,
// RegexMatchSet.RegexMatchSetId, RuleGroup.RuleGroupId,
// LoggingConfiguration.ResourceArn -- so all 13 are "clean" tables registered
// directly on b.registry, with no secondary index needed (every resource
// family here is flat, globally-unique-ID, no parent-scoped lookups).
//
// Left as plain maps (not store.Table-backed):
//   - changeTokens (map[string]string): value is a bare status string, not *T.
//   - ruleGroupRules (map[string][]ActivatedRule): slice-valued and
//     order-sensitive (activated rules within a rule group are sorted by
//     Priority and that order is part of the wire contract), so it does not
//     fit store.Table's keyed-by-identity-value shape and must not be routed
//     through a store.Index (which does not preserve insertion order).
//   - permissionPolicies (map[string]string): value is a bare policy JSON
//     string, not *T.
//   - tags (map[string]map[string]string): value is a bare tag map, not *T.
import "github.com/blackbirdworks/gopherstack/pkgs/store"

func webACLKeyFn(v *WebACL) string { return v.WebACLId }

func ruleKeyFn(v *Rule) string { return v.RuleId }

func rateBasedRuleKeyFn(v *RateBasedRule) string { return v.RuleId }

func ipSetKeyFn(v *IPSet) string { return v.IPSetId }

func byteMatchSetKeyFn(v *ByteMatchSet) string { return v.ByteMatchSetId }

func sizeConstraintSetKeyFn(v *SizeConstraintSet) string { return v.SizeConstraintSetId }

func sqlInjectionMatchSetKeyFn(v *SqlInjectionMatchSet) string { return v.SqlInjectionMatchSetId }

func xssMatchSetKeyFn(v *XssMatchSet) string { return v.XssMatchSetId }

func geoMatchSetKeyFn(v *GeoMatchSet) string { return v.GeoMatchSetId }

func regexPatternSetKeyFn(v *RegexPatternSet) string { return v.RegexPatternSetId }

func regexMatchSetKeyFn(v *RegexMatchSet) string { return v.RegexMatchSetId }

func ruleGroupKeyFn(v *RuleGroup) string { return v.RuleGroupId }

func loggingConfigurationKeyFn(v *LoggingConfiguration) string { return v.ResourceArn }

// registerAllTables constructs and registers every store.Table-backed
// resource field exactly once, at construction time. It must be called
// during construction only, never on every Reset(): store.Register panics on
// a duplicate name, so runtime resets go through b.registry.ResetAll()
// (backend.go) instead.
func registerAllTables(b *InMemoryBackend) {
	b.webACLs = store.Register(b.registry, "webACLs", store.New(webACLKeyFn))
	b.rules = store.Register(b.registry, "rules", store.New(ruleKeyFn))
	b.rateBasedRules = store.Register(b.registry, "rateBasedRules", store.New(rateBasedRuleKeyFn))
	b.ipSets = store.Register(b.registry, "ipSets", store.New(ipSetKeyFn))
	b.byteMatchSets = store.Register(b.registry, "byteMatchSets", store.New(byteMatchSetKeyFn))
	b.sizeConstraintSets = store.Register(b.registry, "sizeConstraintSets", store.New(sizeConstraintSetKeyFn))
	b.sqlInjectionMatchSets = store.Register(
		b.registry, "sqlInjectionMatchSets", store.New(sqlInjectionMatchSetKeyFn),
	)
	b.xssMatchSets = store.Register(b.registry, "xssMatchSets", store.New(xssMatchSetKeyFn))
	b.geoMatchSets = store.Register(b.registry, "geoMatchSets", store.New(geoMatchSetKeyFn))
	b.regexPatternSets = store.Register(b.registry, "regexPatternSets", store.New(regexPatternSetKeyFn))
	b.regexMatchSets = store.Register(b.registry, "regexMatchSets", store.New(regexMatchSetKeyFn))
	b.ruleGroups = store.Register(b.registry, "ruleGroups", store.New(ruleGroupKeyFn))
	b.loggingConfigs = store.Register(b.registry, "loggingConfigs", store.New(loggingConfigurationKeyFn))
}
