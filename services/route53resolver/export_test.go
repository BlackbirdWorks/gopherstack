package route53resolver

// EndpointCount returns the number of resolver endpoints in the backend (test helper).
func EndpointCount(b *InMemoryBackend) int {
	b.mu.RLock("EndpointCount")
	defer b.mu.RUnlock()

	return len(b.endpoints)
}

// RuleCount returns the number of resolver rules in the backend (test helper).
func RuleCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleCount")
	defer b.mu.RUnlock()

	return len(b.rules)
}

// TagCount returns the number of tagged ARNs in the backend (test helper).
func TagCount(b *InMemoryBackend) int {
	b.mu.RLock("TagCount")
	defer b.mu.RUnlock()

	return len(b.tags)
}

// FirewallRuleGroupCount returns the number of firewall rule groups (test helper).
func FirewallRuleGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("FirewallRuleGroupCount")
	defer b.mu.RUnlock()

	return len(b.firewallRuleGroups)
}

// FirewallRuleGroupAssociationCount returns the number of firewall rule group associations (test helper).
func FirewallRuleGroupAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("FirewallRuleGroupAssociationCount")
	defer b.mu.RUnlock()

	return len(b.firewallRuleGroupAssociations)
}

// FirewallDomainListCount returns the number of firewall domain lists (test helper).
func FirewallDomainListCount(b *InMemoryBackend) int {
	b.mu.RLock("FirewallDomainListCount")
	defer b.mu.RUnlock()

	return len(b.firewallDomainLists)
}

// FirewallRuleBackendCount returns the number of firewall rules stored (test helper).
func FirewallRuleBackendCount(b *InMemoryBackend) int {
	b.mu.RLock("FirewallRuleBackendCount")
	defer b.mu.RUnlock()

	return len(b.firewallRules)
}

// OutpostResolverCount returns the number of outpost resolvers (test helper).
func OutpostResolverCount(b *InMemoryBackend) int {
	b.mu.RLock("OutpostResolverCount")
	defer b.mu.RUnlock()

	return len(b.outpostResolvers)
}

// QueryLogConfigCount returns the number of resolver query log configs (test helper).
func QueryLogConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("QueryLogConfigCount")
	defer b.mu.RUnlock()

	return len(b.queryLogConfigs)
}

// QueryLogConfigAssociationCount returns the number of query log config associations (test helper).
func QueryLogConfigAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("QueryLogConfigAssociationCount")
	defer b.mu.RUnlock()

	return len(b.queryLogConfigAssociations)
}

// RuleAssociationCount returns the number of resolver rule associations (test helper).
func RuleAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleAssociationCount")
	defer b.mu.RUnlock()

	return len(b.ruleAssociations)
}

// HandlerOpsLen returns the number of operations registered in the handler dispatch table (test helper).
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
