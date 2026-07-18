package securityhub

func (b *InMemoryBackend) DescribeOrganizationConfiguration() *OrgConfig {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if b.orgConfig == nil {
		return &OrgConfig{
			AutoEnable:                    false,
			MemberAccountLimitReached:     false,
			AutoEnableStandards:           "NONE",
			OrganizationConfigurationType: "CENTRAL",
		}
	}

	cp := *b.orgConfig

	return &cp
}

func (b *InMemoryBackend) UpdateOrganizationConfiguration(
	autoEnable bool,
	autoEnableStandards string,
	orgConfigType string,
) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if b.orgConfig == nil {
		b.orgConfig = &OrgConfig{}
	}

	b.orgConfig.AutoEnable = autoEnable

	if autoEnableStandards != "" {
		b.orgConfig.AutoEnableStandards = autoEnableStandards
	}

	if orgConfigType != "" {
		b.orgConfig.OrganizationConfigurationType = orgConfigType
	}

	return nil
}

func (b *InMemoryBackend) EnableOrganizationAdminAccount(accountID string) error {
	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts[accountID] = statusEnabled

	return nil
}

func (b *InMemoryBackend) DisableOrganizationAdminAccount(accountID string) error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	delete(b.orgAdminAccounts, accountID)

	return nil
}

func (b *InMemoryBackend) ListOrganizationAdminAccounts(nextToken string, maxResults int) ([]*OrgAdminAccount, string) {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	var all []*OrgAdminAccount //nolint:prealloc // existing issue.

	for id, status := range b.orgAdminAccounts {
		all = append(all, &OrgAdminAccount{AccountId: id, Status: status})
	}

	return paginateSlice(all, nextToken, maxResults, maxDefaultResults)
}
