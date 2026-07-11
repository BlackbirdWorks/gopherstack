package iot

// ThingCount returns the number of Things in the backend.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) ThingCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.things.Len()
}

// PolicyCount returns the number of Policies in the backend.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) PolicyCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.policies.Len()
}

// RuleCount returns the number of TopicRules in the backend.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) RuleCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.rules.Len()
}

// CertTransferCount returns the number of certificate transfer records.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) CertTransferCount() int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.certificateTransfers)
}

// ThingPrincipalCount returns the number of principals attached to a thing.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) ThingPrincipalCount(thingName string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.thingPrincipals[thingName])
}

// PolicyTargetCount returns the number of targets a policy is attached to.
// Used only in tests to verify backend state.
func (b *InMemoryBackend) PolicyTargetCount(policyName string) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.policyTargets[policyName])
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations.
// Used only in tests to assert that the dispatch table is fully populated.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// TopicRuleDestConfirmationToken returns the unexported ConfirmationToken for
// a topic rule destination, keyed by ARN. Used only in tests to verify that
// the token (tagged json:"-" so it never leaks into API responses) survives
// a Snapshot/Restore round-trip (gopherstack-264).
func (b *InMemoryBackend) TopicRuleDestConfirmationToken(arn string) string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	dest, ok := b.topicRuleDestinations.Get(arn)
	if !ok {
		return ""
	}

	return dest.ConfirmationToken
}
