package waf

// WebACLCount returns the number of stored WebACLs.
func WebACLCount(b *InMemoryBackend) int {
	b.mu.RLock("WebACLCount")
	defer b.mu.RUnlock()

	return len(b.webACLs)
}

// RuleCount returns the number of stored Rules.
func RuleCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleCount")
	defer b.mu.RUnlock()

	return len(b.rules)
}

// IPSetCount returns the number of stored IPSets.
func IPSetCount(b *InMemoryBackend) int {
	b.mu.RLock("IPSetCount")
	defer b.mu.RUnlock()

	return len(b.ipSets)
}

// ByteMatchSetCount returns the number of stored ByteMatchSets.
func ByteMatchSetCount(b *InMemoryBackend) int {
	b.mu.RLock("ByteMatchSetCount")
	defer b.mu.RUnlock()

	return len(b.byteMatchSets)
}

// SizeConstraintSetCount returns the number of stored SizeConstraintSets.
func SizeConstraintSetCount(b *InMemoryBackend) int {
	b.mu.RLock("SizeConstraintSetCount")
	defer b.mu.RUnlock()

	return len(b.sizeConstraintSets)
}

// SqlInjectionMatchSetCount returns the number of stored SqlInjectionMatchSets.
//
//nolint:revive,staticcheck // AWS SDK naming
func SqlInjectionMatchSetCount(b *InMemoryBackend) int {
	b.mu.RLock("SqlInjectionMatchSetCount")
	defer b.mu.RUnlock()

	return len(b.sqlInjectionMatchSets)
}

// XssMatchSetCount returns the number of stored XssMatchSets.
//
//nolint:revive,staticcheck // AWS SDK naming
func XssMatchSetCount(b *InMemoryBackend) int {
	b.mu.RLock("XssMatchSetCount")
	defer b.mu.RUnlock()

	return len(b.xssMatchSets)
}

// GeoMatchSetCount returns the number of stored GeoMatchSets.
func GeoMatchSetCount(b *InMemoryBackend) int {
	b.mu.RLock("GeoMatchSetCount")
	defer b.mu.RUnlock()

	return len(b.geoMatchSets)
}

// RateBasedRuleCount returns the number of stored RateBasedRules.
func RateBasedRuleCount(b *InMemoryBackend) int {
	b.mu.RLock("RateBasedRuleCount")
	defer b.mu.RUnlock()

	return len(b.rateBasedRules)
}

// RegexPatternSetCount returns the number of stored RegexPatternSets.
func RegexPatternSetCount(b *InMemoryBackend) int {
	b.mu.RLock("RegexPatternSetCount")
	defer b.mu.RUnlock()

	return len(b.regexPatternSets)
}

// RegexMatchSetCount returns the number of stored RegexMatchSets.
func RegexMatchSetCount(b *InMemoryBackend) int {
	b.mu.RLock("RegexMatchSetCount")
	defer b.mu.RUnlock()

	return len(b.regexMatchSets)
}

// RuleGroupCount returns the number of stored RuleGroups.
func RuleGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleGroupCount")
	defer b.mu.RUnlock()

	return len(b.ruleGroups)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
