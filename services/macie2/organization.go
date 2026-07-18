package macie2

import "sort"

// EnableOrganizationAdminAccount designates an account as org admin.
func (b *InMemoryBackend) EnableOrganizationAdminAccount(accountID string) error {
	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts.Put(&OrgAdminAccount{
		AccountID: accountID,
		Status:    "ENABLED",
	})

	return nil
}

// DisableOrganizationAdminAccount removes the org admin designation.
func (b *InMemoryBackend) DisableOrganizationAdminAccount(accountID string) error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	if !b.orgAdminAccounts.Delete(accountID) {
		return ErrOrgAdminNotFound
	}

	return nil
}

// ListOrganizationAdminAccounts returns all org admin accounts.
func (b *InMemoryBackend) ListOrganizationAdminAccounts() ([]*OrgAdminAccount, error) {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	accts := b.orgAdminAccounts.All()
	result := make([]*OrgAdminAccount, 0, len(accts))

	for _, acct := range accts {
		cp := *acct
		result = append(result, &cp)
	}

	sort.Slice(result, func(i, j int) bool { return result[i].AccountID < result[j].AccountID })

	return result, nil
}

// DescribeOrganizationConfiguration returns the org-level Macie configuration.
func (b *InMemoryBackend) DescribeOrganizationConfiguration() (*OrgConfig, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if b.orgConfig == nil {
		return &OrgConfig{}, nil
	}

	cp := *b.orgConfig

	return &cp, nil
}

// UpdateOrganizationConfiguration updates the org-level configuration.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(autoEnable bool) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if b.orgConfig == nil {
		b.orgConfig = &OrgConfig{}
	}

	b.orgConfig.AutoEnable = autoEnable

	return nil
}
