package awsconfig

// PutRemediationConfigurations stores remediation configurations keyed by rule name.
func (b *InMemoryBackend) PutRemediationConfigurations(configs []RemediationConfiguration) error {
	b.mu.Lock("PutRemediationConfigurations")
	defer b.mu.Unlock()

	for i := range configs {
		cp := configs[i]
		b.remediationConfigs.Put(&cp)
	}

	return nil
}

// DescribeRemediationConfigurations returns remediation configurations for the given rule names.
// If ruleNames is empty, all configurations are returned.
func (b *InMemoryBackend) DescribeRemediationConfigurations(ruleNames []string) []RemediationConfiguration {
	b.mu.RLock("DescribeRemediationConfigurations")
	defer b.mu.RUnlock()

	if len(ruleNames) == 0 {
		all := b.remediationConfigs.All()
		out := make([]RemediationConfiguration, 0, len(all))

		for _, rc := range all {
			out = append(out, *rc)
		}

		return out
	}

	out := make([]RemediationConfiguration, 0, len(ruleNames))

	for _, name := range ruleNames {
		if rc, ok := b.remediationConfigs.Get(name); ok {
			out = append(out, *rc)
		}
	}

	return out
}

// PutRemediationExceptions stores a remediation exception for a rule + resource.
func (b *InMemoryBackend) PutRemediationExceptions(ruleName, resourceType, resourceID string) error {
	b.mu.Lock("PutRemediationExceptions")
	defer b.mu.Unlock()

	ex := RemediationException{
		ConfigRuleName: ruleName,
		ResourceType:   resourceType,
		ResourceID:     resourceID,
	}

	existing := b.remediationExceptions[ruleName]

	for i, e := range existing {
		if e.ResourceType == resourceType && e.ResourceID == resourceID {
			existing[i] = ex

			b.remediationExceptions[ruleName] = existing

			return nil
		}
	}

	b.remediationExceptions[ruleName] = append(existing, ex)

	return nil
}

// DescribeRemediationExceptions returns all remediation exceptions for the given rule name.
func (b *InMemoryBackend) DescribeRemediationExceptions(ruleName string) []RemediationException {
	b.mu.RLock("DescribeRemediationExceptions")
	defer b.mu.RUnlock()

	exs := b.remediationExceptions[ruleName]
	if len(exs) == 0 {
		return []RemediationException{}
	}

	out := make([]RemediationException, len(exs))
	copy(out, exs)

	return out
}

// DeleteRemediationConfiguration removes the remediation configuration for the given rule.
func (b *InMemoryBackend) DeleteRemediationConfiguration(ruleName string) error {
	b.mu.Lock("DeleteRemediationConfiguration")
	defer b.mu.Unlock()

	b.remediationConfigs.Delete(ruleName)

	return nil
}

// DeleteRemediationExceptions removes an exception for a rule + resource.
func (b *InMemoryBackend) DeleteRemediationExceptions(ruleName, resourceID string) error {
	b.mu.Lock("DeleteRemediationExceptions")
	defer b.mu.Unlock()

	existing := b.remediationExceptions[ruleName]
	filtered := existing[:0]

	for _, e := range existing {
		if e.ResourceID != resourceID {
			filtered = append(filtered, e)
		}
	}

	b.remediationExceptions[ruleName] = filtered

	return nil
}

// StartRemediationExecution is an intentionally minimal no-op stub.
func (b *InMemoryBackend) StartRemediationExecution() error { return nil }

// DescribeRemediationExecutionStatus returns an empty list (intentionally minimal stub).
func (b *InMemoryBackend) DescribeRemediationExecutionStatus() []any { return []any{} }
