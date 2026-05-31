package securityhub

// FindingCount returns the number of stored findings.
func FindingCount(b *InMemoryBackend) int {
	b.mu.RLock("FindingCount")
	defer b.mu.RUnlock()

	return len(b.findings)
}

// InsightCount returns the number of stored insights.
func InsightCount(b *InMemoryBackend) int {
	b.mu.RLock("InsightCount")
	defer b.mu.RUnlock()

	return len(b.insights)
}

// StandardsSubscriptionCount returns the number of enabled standards subscriptions.
func StandardsSubscriptionCount(b *InMemoryBackend) int {
	b.mu.RLock("StandardsSubscriptionCount")
	defer b.mu.RUnlock()

	return len(b.standardsSubscriptions)
}

// ActionTargetCount returns the number of action targets.
func ActionTargetCount(b *InMemoryBackend) int {
	b.mu.RLock("ActionTargetCount")
	defer b.mu.RUnlock()

	return len(b.actionTargets)
}

// AutomationRuleCount returns the number of automation rules.
func AutomationRuleCount(b *InMemoryBackend) int {
	b.mu.RLock("AutomationRuleCount")
	defer b.mu.RUnlock()

	return len(b.automationRules)
}

// IsHubEnabled returns whether the hub is enabled.
func IsHubEnabled(b *InMemoryBackend) bool {
	b.mu.RLock("IsHubEnabled")
	defer b.mu.RUnlock()

	return b.hubEnabled
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
