package guardduty

// EnableOrganizationAdminAccount designates an account as org admin.
func (b *InMemoryBackend) EnableOrganizationAdminAccount(adminAccountID string) error {
	b.mu.Lock("EnableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts.Put(&OrgAdminAccount{
		AdminAccountID: adminAccountID,
		AdminStatus:    "ENABLED",
	})

	return nil
}

// DisableOrganizationAdminAccount removes an account as org admin.
func (b *InMemoryBackend) DisableOrganizationAdminAccount(adminAccountID string) error {
	b.mu.Lock("DisableOrganizationAdminAccount")
	defer b.mu.Unlock()

	b.orgAdminAccounts.Delete(adminAccountID)

	return nil
}

// ListOrganizationAdminAccounts returns all org admin accounts.
func (b *InMemoryBackend) ListOrganizationAdminAccounts() []*OrgAdminAccount {
	b.mu.RLock("ListOrganizationAdminAccounts")
	defer b.mu.RUnlock()

	items := b.orgAdminAccounts.Snapshot()
	all := make([]*OrgAdminAccount, 0, len(items))

	for _, a := range items {
		cp := *a
		all = append(all, &cp)
	}

	return all
}

// DescribeOrganizationConfiguration returns org config for a detector.
func (b *InMemoryBackend) DescribeOrganizationConfiguration(detectorID string) (*OrgConfig, error) {
	b.mu.RLock("DescribeOrganizationConfiguration")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	cfg, ok := b.orgConfigs.Get(detectorID)
	if !ok {
		return &OrgConfig{DataSources: map[string]any{}, Features: []OrgFeature{}}, nil
	}

	cp := *cfg

	return &cp, nil
}

// UpdateOrganizationConfiguration updates org config for a detector.
func (b *InMemoryBackend) UpdateOrganizationConfiguration(
	detectorID string,
	autoEnable bool,
	features []OrgFeature,
) error {
	b.mu.Lock("UpdateOrganizationConfiguration")
	defer b.mu.Unlock()

	if !b.detectors.Has(detectorID) {
		return ErrDetectorNotFound
	}

	existing, ok := b.orgConfigs.Get(detectorID)
	if !ok {
		existing = &OrgConfig{DataSources: map[string]any{}}
		existing.detectorID = detectorID
	}

	existing.AutoEnable = autoEnable
	if features != nil {
		existing.Features = features
	}

	b.orgConfigs.Put(existing)

	return nil
}

// GetOrganizationStatistics returns org-level statistics.
func (b *InMemoryBackend) GetOrganizationStatistics() map[string]any {
	b.mu.RLock("GetOrganizationStatistics")
	defer b.mu.RUnlock()

	return map[string]any{
		"organizationDetails": map[string]any{
			"organizationStatistics": map[string]any{
				"totalAccountsCount":   1,
				"memberAccountsCount":  b.orgAdminAccounts.Len(),
				"enabledAccountsCount": 0,
				"countByFeature":       []any{},
			},
		},
	}
}
