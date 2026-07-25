package wafv2

import (
	"context"
	"sync"
)

// Web ACL capacity unit (WCU) cost constants for each rule statement type,
// sourced from AWS's published per-statement WCU documentation (verified
// 2026-07-23 against
// https://docs.aws.amazon.com/waf/latest/developerguide/waf-rule-statement-type-*.html
// and https://docs.aws.amazon.com/waf/latest/developerguide/aws-waf-capacity-units.html):
//
//   - ByteMatchStatement ("string match"): 2 WCU for EXACTLY/STARTS_WITH/ENDS_WITH,
//     10 WCU for CONTAINS/CONTAINS_WORD.
//   - SqliMatchStatement: 20 WCU (SensitivityLevel LOW, the default) or 30 WCU (HIGH).
//   - XssMatchStatement: 40 WCU base.
//   - SizeConstraintStatement: 1 WCU base.
//   - RegexMatchStatement: 3 WCU base.
//   - RegexPatternSetReferenceStatement: 25 WCU base.
//   - GeoMatchStatement / LabelMatchStatement / AsnMatchStatement: 1 WCU flat.
//   - IPSetReferenceStatement: 1 WCU, +4 WCU if IPSetForwardedIPConfig.Position is ANY.
//   - RateBasedStatement: 2 WCU base, +30 WCU per custom aggregation key, plus the
//     capacity of any ScopeDownStatement.
//   - AndStatement/OrStatement/NotStatement ("logical rule statements"): cost is
//     the sum of the nested statements' capacities -- AWS's docs state this
//     explicitly ("WCUs -- Depends on the nested statements") with no fixed
//     per-statement overhead.
//   - RuleGroupReferenceStatement: the referenced RuleGroup's fixed Capacity
//     (assigned at RuleGroup creation and immutable thereafter, matching AWS's
//     "cost of using a rule group ... is the rule group's capacity setting").
//   - ManagedRuleGroupStatement: the referenced managed rule group's Capacity
//     from the static catalog (managed_rule_catalog.go), or
//     defaultManagedRuleGroupCapacity for a vendor/name pair not in the catalog.
//
// The FieldToMatch-based statement types (ByteMatch/Sqli/Xss/SizeConstraint/
// RegexMatch/RegexPatternSetReference) share one additional rule, confirmed
// identically worded across each of their doc pages: "If you use the request
// component All query parameters, add 10 WCUs. If you use the request
// component JSON body, double the base cost WCUs. For each Text
// transformation that you apply, add 10 WCUs." Because FieldToMatch is a
// oneOf (exactly one request-component field is ever set), the
// AllQueryArguments/JsonBody adjustments are mutually exclusive.
const (
	wcuByteMatchShortBase     = int64(2)  // EXACTLY / STARTS_WITH / ENDS_WITH
	wcuByteMatchContainsBase  = int64(10) // CONTAINS / CONTAINS_WORD
	wcuSqliMatchLowBase       = int64(20)
	wcuSqliMatchHighBase      = int64(30)
	wcuXSSMatchBase           = int64(40)
	wcuSizeConstraintBase     = int64(1)
	wcuRegexMatchBase         = int64(3)
	wcuRegexPatternSetRefBase = int64(25)
	wcuGeoMatch               = int64(1)
	wcuLabelMatch             = int64(1)
	wcuAsnMatch               = int64(1)
	wcuIPSetReferenceBase     = int64(1)
	wcuIPSetReferenceAnyFwd   = int64(4) // extra when IPSetForwardedIPConfig.Position == "ANY"
	wcuAllQueryArgumentsAdd   = int64(10)
	wcuPerTextTransformation  = int64(10)
	wcuRateBasedBase          = int64(2)
	wcuRateBasedPerCustomKey  = int64(30)
	// defaultManagedRuleGroupCapacity is used for a ManagedRuleGroupStatement
	// referencing a vendor/name pair outside the static catalog in
	// managed_rule_catalog.go (e.g. an unmodeled Marketplace subscription).
	// Matches AWSManagedRulesCommonRuleSet, AWS's general-purpose baseline
	// managed rule group.
	defaultManagedRuleGroupCapacity = int64(700)
)

// onceSimpleStatementCapacityFuncs lazily builds the dispatch table for
// self-contained statement types: those whose WCU cost depends only on their
// own body, with no recursion into nested statements and no backend lookups.
// Composite/reference statement types (And/Or/Not/RateBased/
// RuleGroupReference/ManagedRuleGroup) are handled directly in
// statementCapacityLocked since they need recursion and/or backend access.
//
//nolint:gochecknoglobals // read-only package-level lookup table, apigatewayv2 style
var onceSimpleStatementCapacityFuncs = sync.OnceValue(func() map[string]func(map[string]any) int64 {
	return map[string]func(map[string]any) int64{
		"ByteMatchStatement":                byteMatchCapacity,
		"SqliMatchStatement":                sqliMatchCapacity,
		"XssMatchStatement":                 xssMatchCapacity,
		"SizeConstraintStatement":           sizeConstraintCapacity,
		"RegexMatchStatement":               regexMatchCapacity,
		"RegexPatternSetReferenceStatement": regexPatternSetReferenceCapacity,
		"GeoMatchStatement":                 flatCapacity(wcuGeoMatch),
		"LabelMatchStatement":               flatCapacity(wcuLabelMatch),
		"AsnMatchStatement":                 flatCapacity(wcuAsnMatch),
		"IPSetReferenceStatement":           ipSetReferenceCapacity,
	}
})

// CheckCapacity returns the total WCU (web ACL capacity unit) cost of the
// given rules, replicating AWS's real per-statement-type cost model instead
// of a flat per-rule cost (see the constants block above for sourcing).
func (b *InMemoryBackend) CheckCapacity(ctx context.Context, _ string, rules []map[string]any) (int64, error) {
	b.mu.RLock("CheckCapacity")
	defer b.mu.RUnlock()

	region := getRegion(ctx, b.region)

	var total int64
	for _, rule := range rules {
		total += b.ruleCapacityLocked(region, rule)
	}

	return total, nil
}

// ruleCapacityLocked computes the WCU cost of a single Rule object. Must be
// called with b.mu held (read or write).
func (b *InMemoryBackend) ruleCapacityLocked(region string, rule map[string]any) int64 {
	stmt, ok := rule["Statement"].(map[string]any)
	if !ok {
		return wcuPerRule
	}

	return b.statementCapacityLocked(region, stmt)
}

// statementCapacityLocked computes the WCU cost of a single Statement object
// (a map with exactly one AWS statement-type key set, per the Statement
// union). Falls back to 1 WCU for a statement type this emulator doesn't
// recognize, rather than erroring -- CheckCapacity must not fail just
// because it hasn't modeled every current or future statement type. Must be
// called with b.mu held (read or write).
func (b *InMemoryBackend) statementCapacityLocked(region string, stmt map[string]any) int64 {
	for key, fn := range onceSimpleStatementCapacityFuncs() {
		if inner, present := stmt[key].(map[string]any); present {
			return fn(inner)
		}
	}

	if inner, present := stmt["RateBasedStatement"].(map[string]any); present {
		return b.rateBasedCapacityLocked(region, inner)
	}

	if inner, present := stmt["AndStatement"].(map[string]any); present {
		return b.logicalStatementsCapacityLocked(region, inner)
	}

	if inner, present := stmt["OrStatement"].(map[string]any); present {
		return b.logicalStatementsCapacityLocked(region, inner)
	}

	if inner, present := stmt["NotStatement"].(map[string]any); present {
		return b.notStatementCapacityLocked(region, inner)
	}

	if inner, present := stmt["RuleGroupReferenceStatement"].(map[string]any); present {
		return b.ruleGroupReferenceCapacityLocked(inner)
	}

	if inner, present := stmt["ManagedRuleGroupStatement"].(map[string]any); present {
		return managedRuleGroupCapacity(inner)
	}

	return wcuPerRule
}

// fieldToMatchCapacity applies the shared request-component/text-transformation
// surcharge rule to a FieldToMatch-based statement body, given its base cost.
// FieldToMatch is a oneOf, so the AllQueryArguments and JsonBody adjustments
// are mutually exclusive.
func fieldToMatchCapacity(inner map[string]any, base int64) int64 {
	cost := base

	if ftm, ok := inner["FieldToMatch"].(map[string]any); ok {
		switch {
		case has(ftm, "AllQueryArguments"):
			cost += wcuAllQueryArgumentsAdd
		case has(ftm, "JsonBody"):
			cost *= 2
		}
	}

	if transforms, ok := inner["TextTransformations"].([]any); ok {
		cost += int64(len(transforms)) * wcuPerTextTransformation
	}

	return cost
}

// byteMatchCapacity computes a ByteMatchStatement's base cost from its
// PositionalConstraint before applying the FieldToMatch surcharge.
func byteMatchCapacity(inner map[string]any) int64 {
	base := wcuByteMatchShortBase

	switch inner["PositionalConstraint"] {
	case "CONTAINS", "CONTAINS_WORD":
		base = wcuByteMatchContainsBase
	}

	return fieldToMatchCapacity(inner, base)
}

// sqliMatchCapacity computes a SqliMatchStatement's base cost from its
// SensitivityLevel (LOW is AWS's default when unset) before applying the
// FieldToMatch surcharge.
func sqliMatchCapacity(inner map[string]any) int64 {
	base := wcuSqliMatchLowBase
	if inner["SensitivityLevel"] == "HIGH" {
		base = wcuSqliMatchHighBase
	}

	return fieldToMatchCapacity(inner, base)
}

// xssMatchCapacity, sizeConstraintCapacity, regexMatchCapacity, and
// regexPatternSetReferenceCapacity are the remaining FieldToMatch-based
// statement types: each has a fixed base cost (see the constants block
// above) with the shared request-component/text-transformation surcharge.
func xssMatchCapacity(inner map[string]any) int64 {
	return fieldToMatchCapacity(inner, wcuXSSMatchBase)
}

func sizeConstraintCapacity(inner map[string]any) int64 {
	return fieldToMatchCapacity(inner, wcuSizeConstraintBase)
}

func regexMatchCapacity(inner map[string]any) int64 {
	return fieldToMatchCapacity(inner, wcuRegexMatchBase)
}

func regexPatternSetReferenceCapacity(inner map[string]any) int64 {
	return fieldToMatchCapacity(inner, wcuRegexPatternSetRefBase)
}

// flatCapacity returns a capacity function that ignores the statement body
// and always returns cost, for the flat-1-WCU statement types (GeoMatch,
// LabelMatch, AsnMatch).
func flatCapacity(cost int64) func(map[string]any) int64 {
	return func(map[string]any) int64 { return cost }
}

// ipSetReferenceCapacity computes an IPSetReferenceStatement's cost: 1 WCU,
// +4 WCU if it uses IPSetForwardedIPConfig with Position ANY.
func ipSetReferenceCapacity(inner map[string]any) int64 {
	cost := wcuIPSetReferenceBase

	if cfg, ok := inner["IPSetForwardedIPConfig"].(map[string]any); ok && cfg["Position"] == "ANY" {
		cost += wcuIPSetReferenceAnyFwd
	}

	return cost
}

// rateBasedCapacityLocked computes a RateBasedStatement's cost: 2 WCU base,
// +30 WCU per custom aggregation key, plus the capacity of any nested
// ScopeDownStatement.
func (b *InMemoryBackend) rateBasedCapacityLocked(region string, inner map[string]any) int64 {
	cost := wcuRateBasedBase

	if customKeys, ok := inner["CustomKeys"].([]any); ok {
		cost += int64(len(customKeys)) * wcuRateBasedPerCustomKey
	}

	if scopeDown, ok := inner["ScopeDownStatement"].(map[string]any); ok {
		cost += b.statementCapacityLocked(region, scopeDown)
	}

	return cost
}

// logicalStatementsCapacityLocked computes an AndStatement/OrStatement's
// cost: the sum of its nested Statements' capacities, per AWS's docs
// ("WCUs -- Depends on the nested statements").
func (b *InMemoryBackend) logicalStatementsCapacityLocked(region string, inner map[string]any) int64 {
	statements, ok := inner["Statements"].([]any)
	if !ok {
		return 0
	}

	var total int64

	for _, s := range statements {
		if nested, isMap := s.(map[string]any); isMap {
			total += b.statementCapacityLocked(region, nested)
		}
	}

	return total
}

// notStatementCapacityLocked computes a NotStatement's cost: the capacity of
// its single negated Statement, with no additional overhead.
func (b *InMemoryBackend) notStatementCapacityLocked(region string, inner map[string]any) int64 {
	nested, ok := inner["Statement"].(map[string]any)
	if !ok {
		return 0
	}

	return b.statementCapacityLocked(region, nested)
}

// ruleGroupReferenceCapacityLocked returns the referenced RuleGroup's fixed
// Capacity, matching AWS's documented "the cost of using a rule group ... is
// the rule group's capacity setting". Falls back to 1 WCU if the ARN doesn't
// resolve to a known rule group (e.g. a cross-account/out-of-emulator
// reference), rather than failing the whole CheckCapacity call.
func (b *InMemoryBackend) ruleGroupReferenceCapacityLocked(inner map[string]any) int64 {
	arnStr, ok := inner["ARN"].(string)
	if !ok {
		return wcuPerRule
	}

	rgs := b.ruleGroupsByARN.Get(arnStr)
	if len(rgs) == 0 {
		return wcuPerRule
	}

	return rgs[0].Capacity
}

// managedRuleGroupCapacity returns the referenced managed rule group's
// catalog Capacity (see managed_rule_catalog.go), or
// defaultManagedRuleGroupCapacity if the VendorName/Name pair isn't in the
// static catalog.
func managedRuleGroupCapacity(inner map[string]any) int64 {
	vendorName, _ := inner["VendorName"].(string)
	name, _ := inner["Name"].(string)

	for _, mrg := range getManagedRuleGroups() {
		if mrg.VendorName == vendorName && mrg.Name == name {
			return mrg.Capacity
		}
	}

	return defaultManagedRuleGroupCapacity
}

// has reports whether key is present in m with a non-nil value (FieldToMatch
// members are typically empty objects like AllQueryArguments{}, which decode
// to an empty, non-nil map[string]any{} -- distinct from the key being
// entirely absent).
func has(m map[string]any, key string) bool {
	_, ok := m[key]

	return ok
}
