package macie2

import "sort"

// GetAutomatedDiscoveryConfiguration returns the automated discovery config.
func (b *InMemoryBackend) GetAutomatedDiscoveryConfiguration() (*AutoDiscoveryConfig, error) {
	b.mu.RLock("GetAutomatedDiscoveryConfiguration")
	defer b.mu.RUnlock()

	if b.autoDiscoveryConfig == nil {
		return &AutoDiscoveryConfig{Status: statusDisabled}, nil
	}

	cp := *b.autoDiscoveryConfig

	return &cp, nil
}

// UpdateAutomatedDiscoveryConfiguration updates the automated discovery config.
func (b *InMemoryBackend) UpdateAutomatedDiscoveryConfiguration(autoEnableMembers, status string) error {
	b.mu.Lock("UpdateAutomatedDiscoveryConfiguration")
	defer b.mu.Unlock()

	if b.autoDiscoveryConfig == nil {
		b.autoDiscoveryConfig = &AutoDiscoveryConfig{}
	}

	if autoEnableMembers != "" {
		b.autoDiscoveryConfig.AutoEnableOrganizationMembers = autoEnableMembers
	}

	if status != "" {
		b.autoDiscoveryConfig.Status = status
	}

	return nil
}

// ListAutomatedDiscoveryAccounts returns accounts with automated discovery status.
func (b *InMemoryBackend) ListAutomatedDiscoveryAccounts() ([]*AutoDiscoveryAccount, error) {
	b.mu.RLock("ListAutomatedDiscoveryAccounts")
	defer b.mu.RUnlock()

	accts := b.autoDiscoveryAccounts.All()
	result := make([]*AutoDiscoveryAccount, 0, len(accts))

	for _, acct := range accts {
		cp := *acct
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].AccountID < result[j].AccountID })

	return result, nil
}

// BatchUpdateAutomatedDiscoveryAccounts updates automated discovery status for multiple accounts.
func (b *InMemoryBackend) BatchUpdateAutomatedDiscoveryAccounts(updates []AutoDiscoveryAccountUpdate) error {
	b.mu.Lock("BatchUpdateAutomatedDiscoveryAccounts")
	defer b.mu.Unlock()

	for _, u := range updates {
		if acct, ok := b.autoDiscoveryAccounts.Get(u.AccountID); ok {
			acct.Status = u.Status
		} else {
			b.autoDiscoveryAccounts.Put(&AutoDiscoveryAccount{
				AccountID: u.AccountID,
				Status:    u.Status,
			})
		}
	}

	return nil
}
