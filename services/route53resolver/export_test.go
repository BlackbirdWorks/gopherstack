package route53resolver

// EndpointCount returns the number of resolver endpoints across all regions (test helper).
func EndpointCount(b *InMemoryBackend) int {
	b.mu.RLock("EndpointCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.endpoints {
		n += len(m)
	}

	return n
}

// RuleCount returns the number of resolver rules across all regions (test helper).
func RuleCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.rules {
		n += len(m)
	}

	return n
}

// TagCount returns the number of tagged ARNs across all regions (test helper).
func TagCount(b *InMemoryBackend) int {
	b.mu.RLock("TagCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.tags {
		n += len(m)
	}

	return n
}

// FirewallRuleGroupCount returns the number of firewall rule groups across all regions (test helper).
func FirewallRuleGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("FirewallRuleGroupCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.firewallRuleGroups {
		n += len(m)
	}

	return n
}

// FirewallRuleGroupAssociationCount returns the number of firewall rule group
// associations across all regions (test helper).
func FirewallRuleGroupAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("FirewallRuleGroupAssociationCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.firewallRuleGroupAssociations {
		n += len(m)
	}

	return n
}

// FirewallDomainListCount returns the number of firewall domain lists across all regions (test helper).
func FirewallDomainListCount(b *InMemoryBackend) int {
	b.mu.RLock("FirewallDomainListCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.firewallDomainLists {
		n += len(m)
	}

	return n
}

// FirewallRuleBackendCount returns the number of firewall rules stored across all regions (test helper).
func FirewallRuleBackendCount(b *InMemoryBackend) int {
	b.mu.RLock("FirewallRuleBackendCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.firewallRules {
		n += len(m)
	}

	return n
}

// OutpostResolverCount returns the number of outpost resolvers across all regions (test helper).
func OutpostResolverCount(b *InMemoryBackend) int {
	b.mu.RLock("OutpostResolverCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.outpostResolvers {
		n += len(m)
	}

	return n
}

// QueryLogConfigCount returns the number of resolver query log configs across all regions (test helper).
func QueryLogConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("QueryLogConfigCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.queryLogConfigs {
		n += len(m)
	}

	return n
}

// QueryLogConfigAssociationCount returns the number of query log config associations across all regions (test helper).
func QueryLogConfigAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("QueryLogConfigAssociationCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.queryLogConfigAssociations {
		n += len(m)
	}

	return n
}

// RuleAssociationCount returns the number of resolver rule associations across all regions (test helper).
func RuleAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleAssociationCount")
	defer b.mu.RUnlock()

	n := 0
	for _, m := range b.ruleAssociations {
		n += len(m)
	}

	return n
}

// HandlerOpsLen returns the number of operations registered in the handler dispatch table (test helper).
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
